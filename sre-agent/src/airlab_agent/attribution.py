"""Kural tabanlı mekanizma tespiti ve kaynak/etkilenen uygulama atfı.

Her mekanizma yalnız TELEMETRİ kanıtına dayanır; deney/kaos etiketi (experiment_id, lab_hold) girişten silinmiştir.
Nedensellik güveni (causality_confidence) puandan ve veri kapsamından AYRI raporlanır:
  high   = birbirinden bağımsız ≥2 kaynak aynı yönü gösteriyor ve etkilenen uygulamada etki ölçülmüş
  medium = tek kaynak ya da etki ölçülemedi
  low    = yalnız zamansal çakışma / dolaylı işaret
"""
from __future__ import annotations

from typing import Any

from .scoring import latency_ratio


def _g(d: dict | None, k: str, default=None):
    return (d or {}).get(k, default)


def impacted_apps(cur: dict, base: dict, cfg: dict) -> dict[str, dict]:
    out = {}
    im = cfg["impact"]
    for app in cfg["apps"]:
        c, b = cur["apps"].get(app, {}), base["apps"].get(app, {})
        ratio, src, delta = latency_ratio(c, b, cfg)
        reasons = []
        if ratio is not None and ratio >= im["latency_ratio"] and (delta or 0) >= im["latency_min_delta_ms"]:
            reasons.append(f"latency ×{ratio:.1f} ({src}, +{delta:.0f} ms)")
        er, eb = c.get("err_rate"), b.get("err_rate") or 0.0
        if er is not None and er - eb >= im["error_rate_delta"]:
            reasons.append(f"5xx rate {eb:.1%} → {er:.1%}")
        if (c.get("pool_timeouts") or 0) > 0:
            reasons.append(f"pool timeout {c['pool_timeouts']:.0f}")
        if reasons:
            out[app] = {"reasons": reasons, "latency_ratio": ratio, "latency_source": src}
    return out


def load_increase(cur: dict, base: dict, cfg: dict) -> dict[str, float]:
    out = {}
    for app in cfg["apps"]:
        c, b = _g(cur["apps"].get(app), "rpm"), _g(base["apps"].get(app), "rpm")
        if c is not None and b and c - b >= cfg["impact"].get("min_load_rpm_delta", 0):
            out[app] = c / b
    return out


def lock_chain(sources: list[str], edges: list[dict]) -> list[dict]:
    """Kilit bekleme zinciri: kök engelleyiciden başlayıp her seviyede bir önceki halkayı bekleyen uygulamalar.

    Kenarlar "waiter_app, blocker_app'i bekliyor" çiftleridir; uygulamanın kendi içindeki beklemeler (aynı uygulama)
    zinciri oluşturmaz. Dönen liste kök ilk olmak üzere halkalardır:
      [{"app", "level", "waits_on": [{"app", "n"}], "n"}]  (level 0 = kök, waits_on boş)
    Bir uygulama birden fazla seviyeye bağlanıyorsa en kısa yoldaki seviyesi kullanılır (BFS).
    """
    pairs: dict[tuple[str, str], float] = {}
    for e in edges:
        w, b = e.get("waiter_app"), e.get("blocker_app")
        if w and b and w != b:
            pairs[(w, b)] = pairs.get((w, b), 0) + (e.get("n") or 0)
    roots = [a for a in sources if a and a != "?"]
    if not roots or not pairs:
        return []
    level = {a: 0 for a in roots}
    out = [{"app": a, "level": 0, "waits_on": [], "n": 0} for a in roots]
    frontier = list(roots)
    while frontier:
        nxt: dict[str, list] = {}
        for (w, b), n in pairs.items():
            if b in frontier and w not in level:
                nxt.setdefault(w, []).append({"app": b, "n": n})
        if not nxt:
            break
        depth = level[frontier[0]] + 1
        for w in sorted(nxt, key=lambda a: -sum(x["n"] for x in nxt[a])):
            level[w] = depth
            out.append({"app": w, "level": depth, "waits_on": sorted(nxt[w], key=lambda x: -x["n"]),
                        "n": sum(x["n"] for x in nxt[w])})
        frontier = [w for w in nxt]
    return out


def root_sources(rank: dict[str, float], edges: list[dict], max_ratio: float = 0.5) -> list[str]:
    """Kök aday listesinden, kendisi başka uygulamaları yoğun bekleyen ara halkaları ayıklar.

    Ara halkanın tuttuğu oturum örnekleme anında Lock dışında (ör. ClientRead, CPU) yakalanabilir; bu yüzden
    "blocker_wait != Lock" tek başına kök kanıtı değildir. Bir aday, uygulamalar arası beklediği sayı (W)
    kendisinin beklettiği sayının (B) max_ratio katından azsa kök kalır. Hiçbiri kalmazsa W/B oranı en düşük olan
    seçilir. Sıralama rank (kilit dışı engelleme sayısı) azalan.
    """
    waits: dict[str, float] = {}
    waited: dict[str, float] = {}
    for e in edges:
        w, b, n = e.get("waiter_app"), e.get("blocker_app"), e.get("n") or 0
        if w and b and w != b:
            waits[w] = waits.get(w, 0) + n
            waited[b] = waited.get(b, 0) + n
    ordered = sorted(rank, key=rank.get, reverse=True)
    keep = [a for a in ordered if waits.get(a, 0) < max_ratio * waited.get(a, 0) or not waits.get(a)]
    if keep:
        return keep
    return [min(ordered, key=lambda a: waits.get(a, 0) / max(waited.get(a, 0), 1))] if ordered else []


def chain_text(chain: list[dict]) -> str:
    """'reservation → checkin → baggage' (aynı seviyedekiler '+' ile)."""
    levels: dict[int, list[str]] = {}
    for h in chain:
        levels.setdefault(h["level"], []).append(h["app"])
    return " → ".join(" + ".join(levels[k]) for k in sorted(levels))


def _conf(n_sources: int, impact_seen: bool) -> str:
    if n_sources >= 2 and impact_seen:
        return "high"
    if n_sources >= 1:
        return "medium"
    return "low"


def db_cpu_cause(cur_db: dict, base_db: dict, m: dict, lock_active: bool = False) -> dict[str, Any]:
    """DB CPU artışının olası kaynağı: query yükü mü, maintenance (autovacuum / checkpoint yazma I/O'su) mı.

    Dynatrace PostgreSQL extension'ın postgres.* metric'lerine dayanır; iki dönemin karşılaştırmasıdır. Veri yoksa
    "unknown" döner (tahmin yürütülmez). Korelasyondur: CPU'yu doğrudan süreç bazında ölçmez.
    """
    if not cur_db.get("pg_points") or not base_db.get("pg_points"):
        which = "incelenen aralıkta" if not cur_db.get("pg_points") else "önceki dönemde"
        return {"kind": "unknown", "summary": f"postgres.* extension metric'leri {which} yok; ayrım yapılamadı."}

    def pair(key):
        return base_db.get(key), cur_db.get(key)

    def rose(key, ratio, min_delta) -> bool:
        b, c = pair(key)
        return c is not None and (c - (b or 0.0)) >= min_delta and c >= (b or 0.0) * ratio

    query_up = rose("pg_active_ms_per_min", m["db_cpu_query_ratio"], m["db_cpu_query_min_ms"])
    maint = []
    if rose("pg_autovacuum_blocks_per_min", m["db_cpu_maint_ratio"], m["db_cpu_autovacuum_min_blocks"]):
        maint.append("autovacuum buffer activity")
    if rose("pg_checkpoint_blocks_per_min", m["db_cpu_maint_ratio"], m["db_cpu_checkpoint_min_blocks"]):
        maint.append("checkpoint write I/O")
    kind = ("mixed" if query_up and maint else "query_load" if query_up else "maintenance" if maint
            else "undetermined")
    rnd = lambda p: [None if x is None else round(x, 1) for x in p]  # noqa: E731
    out = {"kind": kind, "maintenance_signals": maint,
           "query_active_ms_per_min": rnd(pair("pg_active_ms_per_min")),            # [önceki dönem, incelenen aralık]
           "autovacuum_blocks_per_min": rnd(pair("pg_autovacuum_blocks_per_min")),
           "checkpoint_blocks_per_min": rnd(pair("pg_checkpoint_blocks_per_min")),
           "tuples_written_per_min": rnd(pair("pg_tup_write_per_min"))}
    out["summary"] = {
        "query_load": "Query active time arttı, maintenance aktivitesi belirgin artmadı: CPU artışı query yüküyle uyumlu.",
        "maintenance": f"Maintenance aktivitesi arttı ({', '.join(maint)}), query active time belirgin artmadı: "
                       "CPU artışı maintenance ile uyumlu.",
        "mixed": f"Hem query active time hem maintenance aktivitesi ({', '.join(maint)}) arttı; payları ayrıştırılamadı.",
        "undetermined": "Ne query active time ne de autovacuum / checkpoint I/O aktivitesi belirgin arttı; "
                        "CPU artışı postgres.* metric'leriyle açıklanamadı.",
    }[kind]
    if lock_active and query_up:        # active_time Lock bekleyen (state=active) session süresini de içerir
        out["caveat"] = "Query active time, Lock Wait'te geçen süreyi de içerir; artışın bir kısmı CPU değil bekleme olabilir."
    return out


# Paylaşılan PostgreSQL'de ölçülen mechanism'ler: bunlardan biri varken "bağımsız load" ya da "network path" denmez.
DB_WORKLOAD = ("temp_file_pressure", "write_pressure", "update_churn", "read_pressure", "query_flood")
LOCK_IDS = ("row_lock", "table_lock", "advisory_lock")
SHARED = {*LOCK_IDS, "global_connection_pressure", "db_cpu", "app_vm_cpu", "deadlock", "serialization_conflict",
          "db_memory_pressure", *DB_WORKLOAD}


def exec_sources(cur: dict, base: dict, cfg: dict) -> tuple[list[str], dict[str, float]]:
    """pg_stat_statements exec time'ı (role bazında) belirgin artan uygulamalar: en büyük artışın en az dörtte biri
    ve asgari eşik. Artış yoksa boş liste (source tahmin edilmez)."""
    incs = {}
    for app in cfg["apps"]:
        c, b = _g(cur["apps"].get(app), "db_exec_ms_per_min"), _g(base["apps"].get(app), "db_exec_ms_per_min")
        if c is not None:
            incs[app] = c - (b or 0)
    floor = max(cfg["mechanisms"]["db_source_min_exec_ms"], max(incs.values(), default=0) * 0.25)
    return [a for a in sorted(sorted(incs), key=incs.get, reverse=True) if incs[a] >= floor], incs


def call_sources(cur: dict, base: dict, cfg: dict) -> dict[str, list[int]]:
    """pg_stat_statements çağrı sayısı (role bazında) belirgin artan uygulamalar → {uygulama: [önceki, incelenen]}
    (çağrı/dk). Çok sayıda küçük statement exec time'da iz bırakmaz (ölçüm: commit storm'da role başına exec time
    artışı ~360 ms/dk iken çağrı sayısı 600 → 15.400/dk); böyle yüklerde source çağrı hacminden okunur."""
    m, out = cfg["mechanisms"], {}
    for app in cfg["apps"]:
        calls = _g(cur["apps"].get(app), "db_calls_per_min")
        before = _g(base["apps"].get(app), "db_calls_per_min") or 0.0
        if calls and calls >= m["flood_min_calls_per_min"] and calls >= before * m["flood_ratio"]:
            out[app] = [round(before), round(calls)]
    return out


def db_workload(cur: dict, base: dict, cfg: dict, imp: dict, ev) -> list[dict]:
    """PostgreSQL workload mechanism'leri: deadlock, serialization failure, temp file, write/WAL, update churn,
    read (scan) ve DB_VM memory. Hepsi iki dönemin karşılaştırmasıdır; veri yoksa mechanism üretilmez."""
    m = cfg["mechanisms"]
    db, bdb = cur["db"], base["db"]
    out: list[dict] = []
    aff = sorted(imp)
    srcs, incs = exec_sources(cur, base, cfg)
    exec_inc = {k: round(v, 1) for k, v in incs.items()}
    vol = call_sources(cur, base, cfg)
    by_calls = sorted(sorted(vol), key=lambda a: vol[a][1], reverse=True)
    mb = 1024 * 1024

    def rose(key, min_value, ratio=m["db_rate_ratio"]) -> bool:
        c, b = db.get(key), bdb.get(key)
        return c is not None and c >= min_value and c >= (b or 0.0) * ratio

    def pair(key, scale=1.0, nd=1):                      # [önceki dönem, incelenen aralık]
        return [None if x is None else round(x / scale, nd) for x in (bdb.get(key), db.get(key))]

    def log_sources(key) -> list[str]:
        by = db.get(key) or {}
        top = max(by.values(), default=0)
        return [a for a in sorted(sorted(by), key=by.get, reverse=True) if by[a] >= top * 0.25]

    def event(metric_key, log_key) -> tuple[float, bool]:
        n = max(db.get(metric_key) or 0, db.get(log_key) or 0) if metric_key else (db.get(log_key) or 0)
        rate = max(db.get(f"{k}_per_min") or 0 for k in (metric_key, log_key) if k)
        brate = max(bdb.get(f"{k}_per_min") or 0 for k in (metric_key, log_key) if k)
        return n, rate >= brate * m["db_event_ratio"] and rate > brate

    # a) Deadlock
    n, up = event("deadlocks", "deadlock_logs")
    if n >= m["deadlock_min"] and up:
        sig = ((["airlab.pg.db.deadlocks"] if (db.get("deadlocks") or 0) >= 1 else [])
               + (["PostgreSQL logu: deadlock detected"] if (db.get("deadlock_logs") or 0) >= 1 else []))
        who = log_sources("deadlock_logs_by_app")
        out.append({
            "id": "deadlock", "title": "Deadlock",
            "source_apps": who, "affected_apps": [a for a in who if a in imp],
            "mechanism": "Transaction'lar aynı row'ları farklı sırayla lock'ladı; PostgreSQL deadlock'u tespit edip "
                         "transaction'lardan birini iptal etti. İptal edilen transaction uygulamada error olarak görünür.",
            "deadlocks": [bdb.get("deadlocks"), db.get("deadlocks")],
            "deadlock_logs": [bdb.get("deadlock_logs"), db.get("deadlock_logs")],
            "logs_by_app": db.get("deadlock_logs_by_app") or {},
            "signals": sig, "confidence": _conf(len(sig), bool(imp)), "evidence_ids": ev("db_stats", "pg_error_logs"),
        })

    # b) Serialization failure: rollback metric'i tek başına ayırt etmez (arka plan trafiği de rollback üretir),
    #    bu yüzden kanıt PostgreSQL logudur.
    n, up = event(None, "serialization_logs")
    if n >= m["serialization_min_logs"] and up:
        sig = ["PostgreSQL logu: could not serialize access"]
        if rose("rollbacks_per_min", 1, 1.5):
            sig.append("airlab.pg.db.xact_rollback artışı")
        who = log_sources("serialization_logs_by_app")
        out.append({
            "id": "serialization_conflict", "title": "Serialization failure (transaction conflict)",
            "source_apps": who, "affected_apps": [a for a in who if a in imp],
            "mechanism": "REPEATABLE READ / SERIALIZABLE isolation level'da çalışan transaction'lar aynı row'ları "
                         "eşzamanlı güncelledi; PostgreSQL çakışan transaction'ı 'could not serialize access' ile "
                         "iptal etti. Uygulama retry yapmıyorsa bu error olarak, yapıyorsa ek load olarak görünür.",
            "serialization_logs": [bdb.get("serialization_logs"), db.get("serialization_logs")],
            "logs_by_app": db.get("serialization_logs_by_app") or {},
            "rollbacks_per_min": pair("rollbacks_per_min"),
            "signals": sig, "confidence": _conf(len(sig), bool(imp)), "evidence_ids": ev("pg_error_logs", "db_stats"),
        })

    # c) Temp file
    if rose("temp_bytes_per_min", m["temp_min_mb_per_min"] * mb):
        out.append({
            "id": "temp_file_pressure", "title": "PostgreSQL temp file pressure",
            "source_apps": srcs, "affected_apps": aff,
            "mechanism": "Sort / hash işlemleri work_mem'e sığmadı ve temp dosyasına taştı. Temp dosyası yazımı disk "
                         "I/O'su ve CPU harcar; aynı disk'i kullanan diğer query'ler de yavaşlayabilir.",
            "temp_mb_per_min": pair("temp_bytes_per_min", mb), "exec_ms_increase_per_min": exec_inc,
            "signals": ["airlab.pg.db.temp_bytes"] + (["pg_stat_statements exec_time"] if srcs else []),
            "confidence": _conf(1 + bool(srcs), bool(imp)), "evidence_ids": ev("db_stats", "statements"),
        })

    # d) Update churn / vacuum
    churn = rose("pg_tup_upd_per_min", m["update_min_per_min"])
    idle_rise = sum(max(0.0, (_g(cur["apps"].get(a), "db_sessions_idle_tx_tw") or 0.0)
                        - (_g(base["apps"].get(a), "db_sessions_idle_tx_tw") or 0.0)) for a in cfg["apps"])
    if churn:
        vac = rose("pg_autovacuum_blocks_per_min", 5000, m["db_cpu_maint_ratio"])
        held = idle_rise >= m["idle_tx_sessions"]
        out.append({
            "id": "update_churn", "title": "UPDATE churn / vacuum pressure",
            "source_apps": srcs, "affected_apps": aff,
            "mechanism": "Çok sayıda row sürekli UPDATE edildi; her UPDATE eski row sürümünü dead tuple olarak bırakır "
                         "ve WAL üretir. Dead tuple'lar vacuum ile temizlenene kadar tablo ve index'ler büyür (bloat), "
                         "query'ler daha fazla blok okur."
                         + (" Aynı aralıkta idle in transaction session'ları da arttı: eski snapshot'lar açıkken vacuum "
                            "bu dead tuple'ları temizleyemez." if held else ""),
            "updates_per_min": pair("pg_tup_upd_per_min", nd=0), "wal_mb_per_min": pair("pg_wal_bytes_per_min", mb),
            "autovacuum_blocks_per_min": pair("pg_autovacuum_blocks_per_min", nd=0),
            "autovacuum_active": vac, "vacuum_blocked_suspect": held, "idle_in_tx_rise": round(idle_rise, 2),
            "exec_ms_increase_per_min": exec_inc,
            "signals": ["postgres.tup_updated"] + (["postgres.wal_bytes"] if rose("pg_wal_bytes_per_min", m["write_min_wal_mb_per_min"] * mb) else [])
                       + (["autovacuum buffer activity"] if vac else []) + (["pg_stat_statements exec_time"] if srcs else []),
            "confidence": _conf(1 + bool(srcs), bool(imp)),
            "evidence_ids": ev("pg_server", "pg_io", "db_sessions", "statements"),
        })

    # e) Write / WAL: insert artışı, ya da update churn'e bağlanamayan WAL artışı
    ins_up = rose("pg_tup_ins_per_min", m["write_min_inserts_per_min"])
    wal_up = rose("pg_wal_bytes_per_min", m["write_min_wal_mb_per_min"] * mb)
    if ins_up or (wal_up and not churn):
        d_ins = (db.get("pg_tup_ins_per_min") or 0) - (bdb.get("pg_tup_ins_per_min") or 0)
        d_cm = (db.get("commits_per_min") or 0) - (bdb.get("commits_per_min") or 0)
        # commit başına birkaç row: yük WAL hacminden çok commit (WAL flush) sayısındadır
        commit_heavy = ins_up and d_cm > 0 and d_ins / d_cm < 5 and rose("commits_per_min", 1, 2.0)
        # commit storm tek satırlık ucuz statement'lardan oluşur: exec time eşiği aşılmazsa source çağrı hacmidir
        w_calls = by_calls if (commit_heavy and not srcs) else []
        w_srcs = srcs or w_calls
        out.append({
            "id": "write_pressure", "title": "PostgreSQL write / WAL pressure",
            "pattern": "commit_heavy" if commit_heavy else "bulk_write",
            "source_apps": w_srcs, "affected_apps": aff,
            "mechanism": ("Çok sayıda küçük transaction ayrı ayrı commit edildi; her commit WAL flush (fsync) ister. "
                          "Commit'ler WAL yazımında sıraya girer, diğer uygulamaların commit latency'si artabilir."
                          if commit_heavy else
                          "Yüksek hacimde INSERT yapıldı; WAL üretimi ve tablo / index büyümesi arttı. WAL yazımı, "
                          "checkpoint ve disk I/O'su paylaşıldığı için diğer uygulamaların write'ları yavaşlayabilir."),
            "inserts_per_min": pair("pg_tup_ins_per_min", nd=0), "wal_mb_per_min": pair("pg_wal_bytes_per_min", mb),
            "commits_per_min": pair("commits_per_min", nd=0),
            "checkpoint_blocks_per_min": pair("pg_checkpoint_blocks_per_min", nd=0),
            "exec_ms_increase_per_min": exec_inc,
            **({"calls_per_min_by_app": {a: vol[a] for a in w_calls}} if w_calls else {}),
            "signals": (["postgres.tup_inserted"] if ins_up else []) + (["postgres.wal_bytes"] if wal_up else [])
                       + (["pg_stat_statements exec_time"] if srcs else [])
                       + (["pg_stat_statements calls"] if w_calls else []),
            "confidence": _conf(ins_up + wal_up + bool(w_srcs), bool(imp)),
            "evidence_ids": ev("pg_server", "db_stats", "statements"),
        })

    # f) Read (scan): okunan tuple hacmi ya da diskten okunan blok
    scan_up = rose("pg_tup_read_per_min", m["read_min_tuples_per_min"])
    disk_up = rose("blks_read_per_min", m["read_min_blocks_per_min"])
    if scan_up or disk_up:
        out.append({
            "id": "read_pressure", "title": "PostgreSQL read (scan) pressure",
            "source_apps": srcs, "affected_apps": aff,
            "mechanism": "Query'ler çok sayıda row taradı (ör. sequential scan). "
                         + ("Okumaların bir kısmı diskten yapıldı; disk I/O'su ve shared_buffers içeriği diğer "
                            "query'lerle paylaşıldığı için onların da latency'si artabilir." if disk_up else
                            "Okumalar shared_buffers'tan karşılandı (diskten okuma artmadı); maliyet disk I/O'su "
                            "değil CPU ve memory bandwidth'tir."),
            "tuples_read_per_min": pair("pg_tup_read_per_min", nd=0),
            "disk_blocks_read_per_min": pair("blks_read_per_min", nd=0),
            "client_blocks_per_min": pair("pg_client_blocks_per_min", nd=0), "disk_reads": disk_up,
            "exec_ms_increase_per_min": exec_inc,
            "signals": (["postgres.tup_returned"] if scan_up else []) + (["airlab.pg.db.blks_read"] if disk_up else [])
                       + (["pg_stat_statements exec_time"] if srcs else []),
            "confidence": _conf(scan_up + disk_up + bool(srcs), bool(imp)),
            "evidence_ids": ev("pg_server", "db_stats", "pg_io", "statements"),
        })

    # h) Query flood: çok sayıda küçük query (write / update yükü zaten bulunduysa ayrıca yazılmaz)
    if not {"write_pressure", "update_churn"} & {x["id"] for x in out}:
        flood = {}
        for app in by_calls:
            c = cur["apps"].get(app) or {}
            calls, ex = c.get("db_calls_per_min"), c.get("db_exec_ms_per_min")
            if ex is not None and ex / calls <= m["flood_max_exec_ms_per_call"]:
                flood[app] = vol[app]
        if flood:
            commits_up = rose("commits_per_min", 1, 2.0)
            who = sorted(sorted(flood), key=lambda a: flood[a][1], reverse=True)
            out.append({
                "id": "query_flood", "title": "Query flood (çok sayıda küçük query)",
                "source_apps": who, "affected_apps": aff,
                "mechanism": "Uygulama çok sayıda küçük, hızlı query gönderiyor (ör. döngü içinde tek tek primary key "
                             "lookup, N+1 pattern). Tek tek ucuz olan query'ler toplamda CPU, network round-trip ve "
                             "connection harcar; her biri ayrı transaction ise commit sayısı da artar.",
                "calls_per_min_by_app": flood, "commits_per_min": pair("commits_per_min", nd=0),
                "tuples_read_per_min": pair("pg_tup_read_per_min", nd=0),
                "signals": ["pg_stat_statements calls"] + (["airlab.pg.db.xact_commit artışı"] if commits_up else []),
                "confidence": _conf(1 + commits_up, bool(imp)), "evidence_ids": ev("statements", "db_stats", "pg_server"),
            })

    # g) DB_VM memory
    dbh, bdbh = cur["hosts"].get(cfg["hosts"]["db"]) or {}, base["hosts"].get(cfg["hosts"]["db"]) or {}
    now, before = dbh.get("mem_max", dbh.get("mem")), bdbh.get("mem")
    rise = (now - before) if (now is not None and before is not None) else None
    if now is not None and (now >= m["db_mem_pct"] or (rise or 0) >= m["db_mem_rise_pp"]):
        busy = rose("pg_active_ms_per_min", m["db_cpu_query_min_ms"], m["db_cpu_query_ratio"])
        strong = now >= m["db_mem_pct"] or (rise or 0) >= 2 * m["db_mem_rise_pp"]
        out.append({
            "id": "db_memory_pressure", "title": "DB_VM memory pressure",
            "source_apps": srcs if busy else [], "affected_apps": aff,
            "mechanism": "PostgreSQL host'unda memory kullanımı arttı (ör. work_mem içinde yapılan büyük sort / hash "
                         "işlemleri, çok sayıda backend). Memory daralınca page cache küçülür, swap ya da OOM riski "
                         "doğar ve tüm query'ler etkilenebilir.",
            "db_mem_pct_max": round(now, 1), "baseline_mem_pct": round(before, 1) if before is not None else None,
            "rise_pp": round(rise, 1) if rise is not None else None,
            "trigger": "absolute" if now >= m["db_mem_pct"] else "relative_rise",
            "query_active_ms_per_min": pair("pg_active_ms_per_min"), "exec_ms_increase_per_min": exec_inc,
            "signals": ["dt.host.memory.usage (db-vm)"] + (["postgres.active_time artışı"] if busy else []),
            "confidence": _conf(1 + busy, bool(imp)) if (strong or busy) else "low",
            "evidence_ids": ev("hosts", "pg_server", "statements"),
        })
    return out


def idle_in_transaction(cur: dict, base: dict, cfg: dict, imp: dict, mechs: list[dict], ev) -> dict | None:
    """Idle in transaction session birikimi (uygulama bazında). Network path latency ile açıklanan uygulamalar hariç."""
    m = cfg["mechanisms"]
    net = {a for x in mechs if x["id"] == "network_path_latency" for a in x["source_apps"]}
    every = {}
    for app in cfg["apps"]:
        r = ((_g(cur["apps"].get(app), "db_sessions_idle_tx_tw") or 0.0)
             - (_g(base["apps"].get(app), "db_sessions_idle_tx_tw") or 0.0))
        if app not in net:
            every[app] = round(r, 2)
    rises = {a: r for a, r in every.items() if r >= m["idle_tx_sessions"]}
    if not rises and sum(max(0.0, r) for r in every.values()) >= 2 * m["idle_tx_sessions"]:
        # hiçbir uygulama tek başına eşiği geçmiyor ama toplam belirgin: payı olan uygulamalar
        rises = {a: r for a, r in every.items() if r >= m["idle_tx_sessions"] / 2}
    if not rises:
        return None
    who = sorted(sorted(rises), key=rises.get, reverse=True)
    return {
        "id": "idle_in_transaction", "title": "Idle in transaction session'ları",
        "source_apps": who, "affected_apps": [a for a in who if a in imp],
        "mechanism": "Uygulama transaction'ı açtı ama commit / rollback etmeden bekliyor. Bu session'lar connection "
                     "tutar, aldıkları lock'ları bırakmaz ve snapshot'ları açık kaldığı için vacuum eski row "
                     "sürümlerini (dead tuple) temizleyemez; süre uzadıkça diğer uygulamalar etkilenir.",
        "idle_in_tx_rise_by_app": rises,
        "signals": ["airlab.pg.sessions (state = idle in transaction)"],
        "confidence": _conf(1, bool(imp)), "evidence_ids": ev("db_sessions"),
    }


def detect(cur: dict, base: dict, cfg: dict, evidence_ids: dict[str, str | None]) -> dict[str, Any]:
    m = cfg["mechanisms"]
    imp = impacted_apps(cur, base, cfg)
    loads = load_increase(cur, base, cfg)
    db, hosts = cur["db"], cur["hosts"]
    bhosts = base["hosts"]
    mechs: list[dict] = []
    ev = lambda *ks: [evidence_ids[k] for k in ks if evidence_ids.get(k)]  # noqa: E731

    # 1) Satır kilidi
    edges = db.get("edges") or []
    edges_total = db.get("edges_total")
    waiter_avg = {a: _g(cur["apps"].get(a), "lock_waiters_avg") for a in cfg["apps"]}
    lock_srcs = []
    if (edges_total or 0) >= m["lock_min_edges"]:
        lock_srcs.append("blocking_edge logları")
    if (db.get("lock_waiters_avg") or 0) >= m["lock_min_waiters_avg"]:
        lock_srcs.append("pg lock waiters metric'i")
    if (db.get("pg_lock_wait_logs") or 0) > 0:
        lock_srcs.append("PostgreSQL log_lock_waits")
    if lock_srcs:
        by_blocker: dict[str, float] = {}
        by_waiter: dict[str, float] = {}
        for e in edges:
            by_blocker[e.get("blocker_app") or "?"] = by_blocker.get(e.get("blocker_app") or "?", 0) + e["n"]
            by_waiter[e.get("waiter_app") or "?"] = by_waiter.get(e.get("waiter_app") or "?", 0) + e["n"]
        for a, v in waiter_avg.items():
            if (v or 0) >= m["lock_min_waiters_avg"]:
                by_waiter.setdefault(a, 0)
        # Kök engelleyici: kendisi Lock beklemeyen blocker (zincirde ara halkalar kaynak sayılmaz)
        root: dict[str, float] = {}
        for e in edges:
            if e.get("blocker_wait") != "Lock":
                root[e.get("blocker_app") or "?"] = root.get(e.get("blocker_app") or "?", 0) + e["n"]
        rank = root or by_blocker
        sources = root_sources(rank, edges)
        chain = sorted(set(by_blocker) - set(sources))
        # Etkilenen: bekleyen uygulamalar; kaynak uygulama yalnız kendisinde de etki ölçüldüyse etkilenen sayılır
        affected = [a for a in sorted(by_waiter, key=lambda a: by_waiter[a], reverse=True)
                    if a not in sources or a in imp]
        blocker_states = sorted({e.get("blocker_state") for e in edges if e.get("blocker_state")})
        chain_hops = lock_chain(sources, edges)
        depth = max((h["level"] for h in chain_hops), default=0)
        # Lock türü: blocking edge'lerdeki wait_event (relation → tablo, advisory → advisory, diğerleri → row).
        # Alan yoksa (eski kayıt) row lock sayılır. 2026-11-27 ölçümü: table lock yükünde tüm edge'ler `relation`,
        # advisory lock yükünde tümü `advisory`, deadlock / row lock yükünde `transactionid`.
        kinds: dict[str, float] = {}
        for e in edges:
            k = {"relation": "table", "advisory": "advisory"}.get(e.get("wait_event"), "row")
            kinds[k] = kinds.get(k, 0) + e["n"]
        total = sum(kinds.values())
        kind = next((k for k in ("table", "advisory") if total and kinds.get(k, 0) >= total * 0.5), "row")
        name = {"row": "Row lock wait", "table": "Table lock wait", "advisory": "Advisory lock wait"}[kind]
        text = {
            "row": "Problemi başlatan uygulamanın açık transaction'ı row lock'u tutarken etkilenen uygulamanın "
                   "UPDATE/SELECT FOR UPDATE işlemleri Lock/transactionid wait'e düşüyor; bekleyen request'ler "
                   "Hikari connection'ını tuttuğu için connection pool doluluğu ve latency artıyor.",
            "table": "Bir transaction tabloyu güçlü bir lock mode'unda tutuyor (ör. ACCESS EXCLUSIVE: DDL, LOCK TABLE, "
                     "TRUNCATE). O tabloya erişen tüm query'ler — SELECT dahil — Lock/relation wait'e düşüyor ve "
                     "arkasında sıraya giriyor; bekleyen request'ler connection tuttuğu için connection pool doluluğu "
                     "ve latency artıyor.",
            "advisory": "Uygulamalar aynı advisory lock key'i için bekliyor (pg_advisory_lock). Advisory lock'lar "
                        "uygulamanın kendi tanımladığı lock'lardır; tabloya ya da row'a bağlı değildir. Lock'u tutan "
                        "session bırakana kadar diğerleri Lock/advisory wait'te kalır.",
        }[kind]
        mechs.append({
            "id": f"{kind}_lock",
            "title": ("Zincirleme " + name[0].lower() + name[1:]) if depth >= 2 else name,
            "source_apps": sources, "affected_apps": affected,
            "mechanism": text, "lock_kind": kind, "lock_kinds": {k: round(v) for k, v in kinds.items()},
            "signals": lock_srcs, "edges_total": edges_total, "edge_pairs": edges[:10],
            "blocker_states": blocker_states, "chain_blockers": chain,
            "lock_chain": chain_hops, "chain_depth": depth, "chain_path": chain_text(chain_hops) if depth else None,
            **({"cascade": "Bekleyen her halka kendi transaction'ında tuttuğu row lock'u ve Hikari connection'ını "
                           "bırakamadığı için bir sonraki uygulamayı bekletiyor; etki, problemi başlatan uygulamadan zincir "
                           "boyunca yayılıyor."} if depth >= 2 else {}),
            "confidence": _conf(len(lock_srcs), any(a in imp for a in affected)),
            "evidence_ids": ev("edges", "collector", "lock_waiters", "pg_lock_logs"),
        })

    # 2) Tek uygulama havuz doygunluğu (DB genelinde baskı yokken)
    conn_ratio = db.get("conn_ratio")
    lock_affected = set(mechs[0]["affected_apps"]) if mechs else set()
    for app in cfg["apps"]:
        c = cur["apps"].get(app, {})
        pr = c.get("pool_ratio_avg", c.get("pool_ratio"))
        if pr is None:
            continue
        sat = pr >= m["pool_active_ratio"] or (c.get("pool_pending_max") or 0) > 0 or (c.get("pool_timeouts") or 0) > 0
        if sat and (conn_ratio is None or conn_ratio < m["db_conn_ratio"]):
            own_load = loads.get(app, 1.0) >= cfg["impact"]["throughput_load_ratio"]
            secondary = app in lock_affected
            srcs = ["Hikari aktif/max"] + (["istek hızı artışı"] if own_load else [])
            mechs.append({
                "id": "pool_saturation", "title": f"{app} connection pool saturation",
                "source_apps": [app] if (own_load or not secondary) else (mechs[0]["source_apps"] if mechs else []),
                "affected_apps": [app],
                "mechanism": ("Lock wait'in sonucu: bekleyen request'ler connection'ları tutuyor." if secondary else
                              "Uygulamanın kendi load'u connection pool kapasitesini aşıyor; DB genelinde connection pressure yok, "
                              "diğer uygulamalara yayılma beklenmez."),
                "secondary_to": mechs[0]["id"] if secondary else None,
                "pool_ratio": round(pr, 3), "pending_max": c.get("pool_pending_max"),
                "timeouts": c.get("pool_timeouts"), "rpm_ratio": round(loads.get(app, 0), 2) if app in loads else None,
                "signals": srcs, "confidence": _conf(len(srcs), app in imp),
                "evidence_ids": ev("hikari", "http"),
            })

    # 3) Global bağlantı baskısı: mutlak eşik VEYA belirgin göreli artış (ör. %31 → %63; 2026-11-24 chaos eval S10)
    base_conn = _g(base["db"], "conn_ratio")
    conn_rise = (conn_ratio - base_conn) if (conn_ratio is not None and base_conn is not None) else None
    conn_rel = (conn_ratio is not None and conn_rise is not None and conn_ratio >= m["db_conn_ratio_rel_min"]
                and conn_rise >= m["db_conn_rise"])
    if conn_ratio is not None and (conn_ratio >= m["db_conn_ratio"] or conn_rel):
        deltas = {}
        for app in cfg["apps"]:
            cs, bs = _g(cur["apps"].get(app), "db_sessions"), _g(base["apps"].get(app), "db_sessions")
            if cs is not None:
                deltas[app] = cs - (bs or 0)
        sources = [a for a in sorted(deltas, key=deltas.get, reverse=True) if deltas[a] > 1][:2]
        mechs.append({
            "id": "global_connection_pressure", "title": "PostgreSQL global connection pressure",
            "source_apps": sources, "affected_apps": sorted(imp),
            "mechanism": "max_connections'a yaklaşıldı; yeni connection açmak isteyen tüm uygulamalar etkilenebilir "
                         "(connection pool'u zaten dolu olanlar daha az).",
            "conn_ratio": round(conn_ratio, 3),
            "baseline_conn_ratio": round(base_conn, 3) if base_conn is not None else None,
            "trigger": "absolute" if conn_ratio >= m["db_conn_ratio"] else "relative_rise",
            "session_delta_by_app": {k: round(v, 2) for k, v in deltas.items()},
            "signals": ["airlab.pg.connections.used_ratio_normal"] + (["session artışı (uygulama bazında)"] if sources else []),
            "confidence": _conf(1 + bool(sources), bool(imp)), "evidence_ids": ev("db_conn", "db_sessions"),
        })

    # 3b) PostgreSQL workload mechanism'leri (DB CPU'dan önce: CPU artışı çoğu zaman bunların sonucudur)
    mechs += db_workload(cur, base, cfg, imp, ev)
    # Deadlock'a giren transaction'lar kısa süre birbirini bekler; bu lock wait'ler deadlock'un parçasıdır.
    # Deadlock daha özgül bulgu olduğu için öne alınır (2026-11-27 ölçümü: 48 deadlock ile birlikte 15 lock edge).
    mechs.sort(key=lambda x: x["id"] != "deadlock")

    # 4) DB_VM CPU
    dbh, bdbh = hosts.get(cfg["hosts"]["db"]) or {}, bhosts.get(cfg["hosts"]["db"]) or {}
    if dbh.get("cpu") is not None and (dbh["cpu"] >= m["db_cpu_pct"] or
                                       dbh["cpu"] - (bdbh.get("cpu") or dbh["cpu"]) >= m["cpu_increase_pp"]):
        incs = {}
        for app in cfg["apps"]:
            c, b = _g(cur["apps"].get(app), "db_exec_ms_per_min"), _g(base["apps"].get(app), "db_exec_ms_per_min")
            if c is not None:
                incs[app] = c - (b or 0)
        sources = [a for a in sorted(incs, key=incs.get, reverse=True) if incs[a] > 0][:2]
        cause = db_cpu_cause(db, base["db"], m, lock_active=any(x["id"] in LOCK_IDS for x in mechs))
        # Neden maintenance ise CPU'yu uygulama query'leri harcamıyor: exec time artışı CPU sıkışmasının sonucudur,
        # bu yüzden source uygulama gösterilmez (2026-11-27 gerçek autovacuum olayında görüldü).
        maint = cause["kind"] == "maintenance"
        if maint:
            sources = []
        mechs.append({
            "id": "db_cpu", "title": "DB_VM CPU pressure", "cause": cause,
            "source_apps": sources, "affected_apps": sorted(imp),
            "mechanism": ("PostgreSQL host'unda CPU saturation tüm query'lerin execution time'ını uzatır; CPU artışı "
                          "maintenance aktivitesiyle uyumlu olduğu için problemi başlatan bir uygulama gösterilmez, uygulamalardaki "
                          "execution time artışı CPU pressure'ın sonucu sayılır." if maint else
                          "PostgreSQL host'unda CPU saturation tüm query'lerin execution time'ını uzatır; problemi başlatan, "
                          "pg_stat_statements execution time'ı en çok artan role(ler)dir."),
            "related_load": [x["id"] for x in mechs if x["id"] in DB_WORKLOAD],
            "db_cpu": round(dbh["cpu"], 1), "baseline_cpu": round(bdbh["cpu"], 1) if bdbh.get("cpu") is not None else None,
            "exec_ms_increase_per_min": {k: round(v, 1) for k, v in incs.items()},
            "signals": ["dt.host.cpu.usage (db-vm)"] + (["pg_stat_statements exec_time"] if sources else [])
                       + (["postgres.active_time / postgres.io (extension)"] if cause["kind"] != "unknown" else []),
            "confidence": _conf(1 + bool(sources), bool(imp)),
            "evidence_ids": ev("hosts", "statements", "pg_server", "pg_io"),
        })

    # 5) APP_VM CPU (agent'ın kendi tüketimi dahil ayrıştırılır)
    aph, baph = hosts.get(cfg["hosts"]["app"]) or {}, bhosts.get(cfg["hosts"]["app"]) or {}
    if aph.get("cpu") is not None and (aph["cpu"] >= m["app_cpu_pct"] or
                                       aph["cpu"] - (baph.get("cpu") or aph["cpu"]) >= m["cpu_increase_pp"]):
        cc, bc = cur["containers"], base["containers"]
        incs = {k: v["cpu_cores"] - (_g(bc.get(k), "cpu_cores") or 0) for k, v in cc.items()}
        top = [k for k in sorted(incs, key=incs.get, reverse=True) if incs[k] > 0.1][:2]
        mechs.append({
            "id": "app_vm_cpu", "title": "APP_VM CPU pressure",
            "source_apps": top, "affected_apps": sorted(imp),
            "mechanism": "Tüm Java servisleri aynı host'u paylaşır; CPU saturation DB ile ilgisiz biçimde her "
                         "uygulamanın latency'sini artırır. DB_VM CPU'su normal ise DB kaynaklı değildir.",
            "app_cpu": round(aph["cpu"], 1), "baseline_cpu": round(baph["cpu"], 1) if baph.get("cpu") is not None else None,
            "container_core_increase": {k: round(v, 3) for k, v in incs.items()},
            "self_is_source": "sre-agent" in top,
            "signals": ["dt.host.cpu.usage (app-vm)"] + (["container CPU"] if top else []),
            "confidence": _conf(1 + bool(top), bool(imp)), "evidence_ids": ev("hosts", "containers"),
        })

    # 5b) Tek uygulama container CPU'su: host eşiği aşılmasa da bir iş uygulamasının container'ı belirgin CPU harcıyor
    #     (2026-11-24 chaos eval S11: baggage 0.03 → 0.74 core, host yalnız +11 puan). Yayılma beklenmez.
    cc, bc = cur["containers"], base["containers"]
    host_rise = (aph.get("cpu") - baph.get("cpu")) if (aph.get("cpu") is not None and baph.get("cpu") is not None) \
        else None
    for app in cfg["apps"]:
        cores, bcores = _g(cc.get(app), "cpu_cores"), _g(bc.get(app), "cpu_cores")
        if cores is None or bcores is None or cores - bcores < m["container_cpu_rise_cores"]:
            continue
        srcs = ["container CPU (dt.containers.cpu)"] + (["APP_VM CPU artışı"] if (host_rise or 0) >= 5 else [])
        mechs.append({
            "id": "app_container_cpu", "title": f"{app} container CPU saturation",
            "source_apps": [app], "affected_apps": [app] if app in imp else [],
            "mechanism": "Uygulamanın kendi container'ı CPU harcıyor (ör. CPU-bound iş, sıkışmış thread'ler, GC). "
                         "Host CPU threshold'un altında olduğu için diğer uygulamalara yayılma beklenmez; DB ile ilgisizdir.",
            "cpu_cores": round(cores, 3), "baseline_cpu_cores": round(bcores, 3),
            "app_vm_cpu_rise_pp": round(host_rise, 1) if host_rise is not None else None,
            "signals": srcs, "confidence": _conf(len(srcs), app in imp), "evidence_ids": ev("containers", "hosts"),
        })

    # 5c) Network path latency (app ↔ PostgreSQL): uygulamada latency artıyor, DB server-side execution süresi artmıyor,
    #     uygulamanın oturumları transaction içinde "idle" (DB client'ı bekliyor) ve paylaşılan resource baskısı yok.
    #     (2026-11-24 chaos eval S13: idle-in-transaction 0 → 2.65, exec/call sabit.)
    shared_now = {x["id"] for x in mechs} & SHARED
    if not shared_now:
        for app in sorted(imp):
            c, b = cur["apps"].get(app, {}), base["apps"].get(app, {})
            idle_rise = (c.get("db_sessions_idle_tx") or 0.0) - (b.get("db_sessions_idle_tx") or 0.0)
            ce, cn = c.get("db_exec_ms_per_min"), c.get("db_calls_per_min")
            be, bn = b.get("db_exec_ms_per_min"), b.get("db_calls_per_min")
            if None in (ce, cn, be, bn) or not cn or not bn or not be:
                continue
            exec_ratio = (ce / cn) / max(be / bn, 1e-9)
            if idle_rise < m["net_idle_tx_sessions"] or exec_ratio >= m["net_max_exec_ratio"]:
                continue
            # Network path'te bekleyen session'lar uygulamanın connection pool'undaki connection'lardır. Pool'un
            # kullandığından fazla idle in transaction session varsa bunlar pool dışı / elde tutulan session'lardır
            # (idle_in_transaction mechanism'i); network gecikmesi değildir.
            pool_active = c.get("pool_active_max")
            if pool_active is not None and (c.get("db_sessions_idle_tx") or 0.0) > pool_active + 0.5:
                continue
            if any(x["id"] == "app_container_cpu" and app in x["source_apps"] for x in mechs):
                continue
            mechs.append({
                "id": "network_path_latency", "title": f"{app} ↔ PostgreSQL network path latency",
                "source_apps": [app], "affected_apps": [app],
                "mechanism": "Uygulamanın DB çağrıları yavaşladı ama PostgreSQL server-side execution süresi değişmedi; "
                             "oturumlar transaction içinde client'ı bekliyor (idle in transaction). Latency DB'de değil, "
                             "uygulama ile DB arasındaki network path'inde (veya client tarafında) oluşuyor.",
                "idle_in_tx_rise": round(idle_rise, 2), "db_exec_per_call_ratio": round(exec_ratio, 2),
                "signals": ["pg sessions idle in transaction", "pg_stat_statements exec/call sabit", "uygulama latency"],
                "confidence": "medium", "evidence_ids": ev("db_sessions", "statements", "http", "spans"),
            })
            for x in mechs:                      # aynı uygulamadaki pool doluluğu bu gecikmenin sonucudur
                if x["id"] == "pool_saturation" and app in x["affected_apps"] and not x.get("secondary_to"):
                    x["secondary_to"] = "network_path_latency"

    # 5d) Idle in transaction birikimi
    idle = idle_in_transaction(cur, base, cfg, imp, mechs, ev)
    if idle:
        mechs.append(idle)

    # 6) Bağımsız yük: yük artan uygulama var ama başka uygulamaya yayılma ve paylaşılan kaynak mekanizması yok
    shared = {x["id"] for x in mechs} & (SHARED | {"idle_in_transaction"})
    for app, r in loads.items():
        if r >= cfg["impact"]["throughput_load_ratio"] and not shared:
            others = [a for a in imp if a != app]
            mechs.append({
                "id": "independent_load", "title": f"{app} bağımsız load artışı",
                "source_apps": [app], "affected_apps": [app] if app in imp else [],
                "mechanism": "Request rate arttı; paylaşılan resource'ta (lock, DB connection, CPU) pressure ölçülmedi. "
                             + ("Başka uygulamalarda da etki var — ilişki için evidence yok." if others else
                                "Diğer uygulamalara yayılma gözlenmedi."),
                "rpm_ratio": round(r, 2), "unexplained_impact": others,
                "signals": ["Micrometer istek hızı"], "confidence": "medium" if not others else "low",
                "evidence_ids": ev("http"),
            })

    primary = [x for x in mechs if not x.get("secondary_to")]
    explained = {a for x in mechs for a in x["affected_apps"]}
    return {"impacted": imp, "load_ratio": {k: round(v, 2) for k, v in loads.items()}, "mechanisms": mechs,
            "primary": primary[0]["id"] if primary else None,
            "unexplained_impacted": sorted(set(imp) - explained)}


def alternatives(attr: dict, quality: dict) -> list[str]:
    """Rapor için alternatif açıklamalar ve dikkat notları (deterministik)."""
    ids = {m["id"] for m in attr["mechanisms"]}
    out = []
    if ids & set(LOCK_IDS):                      # aşağıdaki notlar lock türünden bağımsızdır
        ids = ids | {"row_lock"}
    if "row_lock" in ids and "app_vm_cpu" in ids:
        out.append("Latency, lock yerine APP_VM CPU saturation'dan da kaynaklanabilir; iki problem aynı anda aktif.")
    if "row_lock" in ids and "global_connection_pressure" in ids:
        out.append("Connection pressure, lock wait'in sonucu olabilir (bekleyen session'lar connection tutar).")
    if "db_cpu" in ids:
        cause = next(m for m in attr["mechanisms"] if m["id"] == "db_cpu").get("cause") or {"kind": "unknown"}
        if cause["kind"] == "unknown" and "row_lock" not in ids:
            out.append("DB CPU artışı ağır query yerine vacuum/checkpoint gibi maintenance job'larından kaynaklanıyor "
                       "olabilir; " + cause.get("summary", "postgres.* extension metric'leri yok; ayrım yapılamadı."))
        elif cause["kind"] in ("maintenance", "mixed", "undetermined"):
            out.append("DB CPU: " + cause["summary"])
        if cause.get("caveat"):
            out.append(cause["caveat"])
        related = [m["title"] for m in attr["mechanisms"] if m["id"] in DB_WORKLOAD]
        if related and cause["kind"] != "maintenance":
            out.append(f"DB CPU artışı, aynı aralıkta ölçülen şu load ile uyumlu: {', '.join(related)}.")
    if "update_churn" in ids:
        out.append("Dead tuple sayısı ve tablo bloat'u doğrudan ölçülmedi; update hızı ve autovacuum aktivitesinden "
                   "çıkarım yapıldı (pg_stat_user_tables ile doğrulayın).")
    if "db_memory_pressure" in ids:
        out.append("DB_VM memory artışı host düzeyinde ölçüldü; PostgreSQL backend'leri dışındaki process'lerden de "
                   "kaynaklanabilir.")
    if "serialization_conflict" in ids or "deadlock" in ids:
        out.append("Deadlock / serialization failure sayıları PostgreSQL logundan okundu; source uygulama, log "
                   "satırındaki database role'ünden çıkarıldı.")
    if attr["unexplained_impacted"]:
        out.append(f"Şu uygulamalardaki etki bilinen problem türlerinden hiçbiriyle açıklanamadı: {', '.join(attr['unexplained_impacted'])}"
                   " (uygulama içi error, GC veya network olabilir).")
    if quality.get("spans", {}).get("status") != "OK":
        out.append("Span verisi eksik/seyrek: latency Micrometer mean'den hesaplandı, p95 değil.")
    if not ids and attr["impacted"]:
        out.append("Etki var ama ölçülen shared resource pressure yok; external dependency veya uygulama içi sorun olabilir.")
    return out
