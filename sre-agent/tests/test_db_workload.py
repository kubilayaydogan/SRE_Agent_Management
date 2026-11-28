"""PostgreSQL workload mechanism'leri: signal, özellik çıkarımı ve ayrıntı alanları (senaryo sonuçları
test_scenarios.py + eval/labels.yaml'dadır)."""
from airlab_agent.attribution import DB_WORKLOAD, SHARED, alternatives, detect
from airlab_agent.features import assess_quality, extract
from airlab_agent.mcp_gateway import validate_dql
from airlab_agent.report import human_review
from airlab_agent.signals import CATALOG, REQUIRED, WATCH_SIGNALS, build_queries
from scenarios import SCENARIOS
from synth import build, cfg, windows


def run(name, **kw):
    c, ws = cfg(), windows()
    base, win, extra = SCENARIOS[name]()
    res = build(base, win, ws, **{**extra, **kw})
    F = extract(res, ws, c)
    q = assess_quality(res, ws[1], c)
    a = detect(F["window"], F["baseline"], c, {k: r.evidence_id for k, r in res.items() if r.status == "OK"})
    return F, q, a


def mech(a, mid):
    return next(m for m in a["mechanisms"] if m["id"] == mid)


def test_error_log_signal_is_supporting_and_read_only():
    assert "pg_error_logs" in CATALOG and "pg_error_logs" not in set(REQUIRED) | set(WATCH_SIGNALS)
    q = dict(build_queries(windows(), cfg(), ["pg_error_logs"]))["pg_error_logs"]
    assert validate_dql(q) is None
    assert "deadlock detected" in q and "could not serialize access" in q and "LD:role" in q
    assert len(CATALOG) <= cfg()["budgets"]["max_queries_per_run"]


def test_features_are_per_minute_and_logs_have_true_zero():
    F, _, _ = run("serialization")
    b, w = F["baseline"]["db"], F["window"]["db"]
    assert (b["rollbacks_per_min"], w["rollbacks_per_min"], w["commits_per_min"]) == (70.0, 226.0, 8700.0)
    assert (b["serialization_logs"], w["serialization_logs"], w["deadlock_logs"]) == (0, 113, 0)
    assert w["serialization_logs_per_min"] == 113 / 5
    assert w["serialization_logs_by_app"] == {"flightops-service": 60, "checkin-service": 53}


def test_log_counts_unknown_when_query_fails():
    F, _, _ = run("serialization", drop=("pg_error_logs",))
    assert F["window"]["db"]["serialization_logs"] == 0                # EMPTY: query yanıt verdi → gerçek 0
    res = build(*SCENARIOS["serialization"]()[:2], windows())
    res["pg_error_logs"].status, res["pg_error_logs"].records = "ERROR", []
    F = extract(res, windows(), cfg())
    assert "serialization_logs" not in F["window"]["db"]               # yanıt yok → bilinmiyor, 0 değil
    a = detect(F["window"], F["baseline"], cfg(), {})
    assert "serialization_conflict" not in [m["id"] for m in a["mechanisms"]]


def test_idle_in_tx_is_time_weighted():
    F, _, a = run("idle_in_tx")
    assert F["window"]["apps"]["checkin-service"]["db_sessions_idle_tx_tw"] == 5.0
    m = mech(a, "idle_in_transaction")
    assert m["idle_in_tx_rise_by_app"] == {"checkin-service": 5.0, "flightops-service": 5.0}
    assert m["affected_apps"] == []                                    # uygulamalarda etki ölçülmedi


def test_network_path_still_wins_when_idle_sessions_fit_the_pool():
    _, _, a = run("network_path")
    ids = [m["id"] for m in a["mechanisms"]]
    assert "network_path_latency" in ids and "idle_in_transaction" not in ids


def test_write_pressure_patterns_and_details():
    _, _, a = run("bulk_insert")
    m = mech(a, "write_pressure")
    assert m["pattern"] == "bulk_write" and m["inserts_per_min"] == [0.0, 1000000.0]
    assert m["wal_mb_per_min"][1] > 700 and "postgres.wal_bytes" in m["signals"]
    _, _, a = run("commit_storm")
    assert mech(a, "write_pressure")["pattern"] == "commit_heavy"


def test_commit_storm_source_falls_back_to_call_volume():
    _, _, a = run("commit_storm")                                      # exec time yeterli: çağrı hacmine bakılmaz
    m = mech(a, "write_pressure")
    assert m["source_apps"] == ["baggage-service"] and "calls_per_min_by_app" not in m
    _, _, a = run("commit_storm_cheap")                                # exec time eşiğin altında
    m = mech(a, "write_pressure")
    assert m["pattern"] == "commit_heavy" and m["source_apps"] == ["flightops-service", "checkin-service"]
    assert m["calls_per_min_by_app"] == {"flightops-service": [10, 16000], "checkin-service": [10, 15400]}
    assert "pg_stat_statements calls" in m["signals"] and "pg_stat_statements exec_time" not in m["signals"]
    assert "query_flood" not in [x["id"] for x in a["mechanisms"]]
    _, _, a = run("bulk_insert", )                                     # bulk write'ta çağrı hacmi source sayılmaz
    assert "calls_per_min_by_app" not in mech(a, "write_pressure")


def test_update_churn_reports_vacuum_context():
    _, _, a = run("update_churn")
    m = mech(a, "update_churn")
    assert m["autovacuum_active"] and not m["vacuum_blocked_suspect"]
    _, _, a = run("vacuum_blocked")
    m = mech(a, "update_churn")
    assert m["vacuum_blocked_suspect"] and "vacuum" in m["mechanism"]
    assert any("Dead tuple" in x for x in alternatives(a, {"spans": {"status": "OK"}}))


def test_read_pressure_links_db_cpu_and_says_where_reads_came_from():
    _, _, a = run("heavy_read")
    m = mech(a, "read_pressure")
    assert m["disk_reads"] is False and "shared_buffers" in m["mechanism"]
    assert mech(a, "db_cpu")["related_load"] == ["read_pressure"]
    assert any(x.startswith("DB CPU artışı, aynı aralıkta") for x in alternatives(a, {"spans": {"status": "OK"}}))


def test_memory_pressure_fields_and_confidence():
    _, _, a = run("db_memory")
    m = mech(a, "db_memory_pressure")
    assert (m["baseline_mem_pct"], m["db_mem_pct_max"], m["rise_pp"], m["trigger"]) == (52.0, 58.0, 6.0, "relative_rise")
    assert m["confidence"] in ("medium", "high")


def test_every_new_mechanism_has_review_guidance_and_evidence():
    seen = set()
    for name in ("deadlock", "serialization", "temp_spill", "bulk_insert", "update_churn", "heavy_read", "db_memory", "point_queries",
                 "idle_in_tx"):
        _, q, a = run(name)
        scores = {"overall": {"score": 10}}
        assert human_review(a, q, scores) != ["Belirgin bir risk yok; rutin monitoring yeterli."], name
        for m in a["mechanisms"]:
            assert m["evidence_ids"] and m["signals"] and m["confidence"] in ("low", "medium", "high"), (name, m["id"])
            seen.add(m["id"])
    assert set(DB_WORKLOAD) | {"deadlock", "serialization_conflict", "db_memory_pressure", "idle_in_transaction"} <= seen
    assert set(DB_WORKLOAD) < SHARED


def test_lock_kind_comes_from_wait_event_and_defaults_to_row():
    q = dict(build_queries(windows(), cfg(), ["edges"]))["edges"]
    assert "wait_event=waiter.wait_event" in q
    _, _, a = run("table_lock")
    m = a["mechanisms"][0]
    assert (m["id"], m["title"], m["lock_kind"]) == ("table_lock", "Table lock wait", "table")
    assert m["lock_kinds"] == {"table": 24, "row": 1} and "ACCESS EXCLUSIVE" in m["mechanism"]
    assert any(x["id"] == "pool_saturation" and x["secondary_to"] == "table_lock" for x in a["mechanisms"])
    _, _, a = run("advisory_lock")
    assert a["mechanisms"][0]["id"] == "advisory_lock" and "pg_advisory_lock" in a["mechanisms"][0]["mechanism"]
    _, _, a = run("row_lock")                          # wait_event transactionid → row lock (önceki davranış)
    assert a["mechanisms"][0]["id"] == "row_lock" and a["mechanisms"][0]["lock_kind"] == "row"


def test_query_flood_needs_many_cheap_calls():
    _, q, a = run("point_queries")
    m = mech(a, "query_flood")
    assert m["calls_per_min_by_app"] == {"checkin-service": [10, 9566], "flightops-service": [10, 9860]}
    assert "airlab.pg.db.xact_commit artışı" in m["signals"]
    assert human_review(a, q, {"overall": {"score": 10}}) != ["Belirgin bir risk yok; rutin monitoring yeterli."]
    _, _, a = run("commit_storm")                      # write pressure zaten açıklıyor
    assert "query_flood" not in [x["id"] for x in a["mechanisms"]]
