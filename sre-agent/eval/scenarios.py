"""Değerlendirme senaryoları — YALNIZ GİRDİ (telemetri biçiminde). Beklenen sonuçlar eval/labels.yaml'dadır ve
agent'a hiçbir aşamada verilmez. Kenar (edge) kayıtlarında experiment_id/lab_hold gibi kaos etiketleri bilerek
bulunur: agent bunları girişte silmeli ve kanıt olarak kullanmamalıdır.
"""
from __future__ import annotations

import copy

from synth import NORMAL


def _w(**over):
    w = copy.deepcopy(NORMAL)
    for k, v in over.items():
        if k == "apps":
            for app, vals in v.items():
                w["apps"][app].update(vals)
        elif k == "hosts":
            w["hosts"].update(v)
        else:
            w[k] = v
    return w


R, C, F, B = "reservation-service", "checkin-service", "flightops-service", "baggage-service"


def _edge(waiter, blocker, n, blocker_wait="Timeout", wait_event="transactionid"):
    return {"waiter_app": waiter, "blocker_app": blocker, "wait_type": "Lock", "wait_event": wait_event,
            "waiter_op": "checkin",
            "blocker_op": "hold", "blocker_state": "active", "blocker_wait": blocker_wait, "n": str(n),
            "wait_max": "4.2", "waiter.experiment_id": "exp-LEAK", "blocker.lab_hold": "hold-inventory-lock"}


def _pg(**over):
    return {**NORMAL["pg"], **over}


SCENARIOS = {
    "normal": lambda: (NORMAL, _w(), {}),
    "independent_load": lambda: (NORMAL, _w(apps={F: {"rpm": 200.0, "mean_ms": 22.0, "p95": 33.0}}), {}),
    "row_lock": lambda: (NORMAL, _w(
        apps={C: {"mean_ms": 900.0, "p95": 3900.0, "pool_a": 7.5, "pool_to": 1.0, "sessions": 8.0}},
        lock_total=6.0, lock_pairs={(C, R): 5.0, (C, C): 1.0},
        edges=[_edge(C, R, 140), _edge(C, C, 60, blocker_wait="Lock"), _edge(R, R, 10)], pg_lock_logs=900), {}),
    "single_pool": lambda: (NORMAL, _w(
        apps={B: {"rpm": 240.0, "pool_a": 8.0, "pool_pe": 4.0, "mean_ms": 300.0, "p95": 600.0}}), {}),
    "global_connection": lambda: (NORMAL, _w(
        conn_ratio=0.92, apps={R: {"sessions": 60.0, "mean_ms": 80.0, "p95": 120.0},
                               F: {"mean_ms": 70.0, "p95": 110.0}}), {}),
    "db_cpu": lambda: (NORMAL, _w(
        hosts={"db-vm": 96.0}, apps={C: {"exec_ms": 30000.0, "mean_ms": 200.0, "p95": 400.0},
                                     R: {"mean_ms": 90.0, "p95": 150.0}}), {}),
    "app_vm_cpu": lambda: (NORMAL, _w(
        hosts={"app-vm": 97.0}, apps={B: {"cores": 1.8}, R: {"mean_ms": 70.0, "p95": 100.0},
                                      F: {"mean_ms": 60.0, "p95": 90.0}}), {}),
    "two_sources": lambda: (NORMAL, _w(
        apps={C: {"mean_ms": 900.0, "p95": 3900.0, "pool_a": 7.5, "sessions": 8.0},
              B: {"rpm": 240.0, "pool_a": 8.0, "pool_pe": 3.0, "mean_ms": 300.0, "p95": 600.0}},
        lock_total=5.0, lock_pairs={(C, R): 5.0}, edges=[_edge(C, R, 120)], pg_lock_logs=500), {}),
    # 2026-11-24 chaos eval sonrası eklenen kurallar (pozitif + negatif)
    "conn_relative": lambda: (_w(conn_ratio=0.30), _w(conn_ratio=0.65, apps={F: {"sessions": 25.0}, B: {"sessions": 12.0}}), {}),
    "conn_small_rise": lambda: (_w(conn_ratio=0.10), _w(conn_ratio=0.45, apps={F: {"sessions": 20.0}}), {}),
    "container_cpu": lambda: (NORMAL, _w(hosts={"app-vm": 41.0}, apps={B: {"cores": 0.95}}), {}),
    "network_path": lambda: (NORMAL, _w(apps={C: {"mean_ms": 300.0, "p95": 650.0, "pool_a": 7.5, "idle_tx": 2.7}}), {}),
    "pool_hold_not_network": lambda: (NORMAL, _w(
        apps={C: {"mean_ms": 600.0, "p95": 1700.0, "pool_a": 8.0, "pool_pe": 6.0, "pool_to": 3.0, "idle_tx": 1.0}}), {}),
    "tail_latency": lambda: (NORMAL, _w(apps={R: {"mean_ms": 150.0, "p95": 32.0}}), {}),
    "missing_telemetry": lambda: (NORMAL, _w(collector=False),
                                  {"drop": ("spans", "edges", "pg_lock_logs", "lock_waiters", "hikari")}),
    "stale_telemetry": lambda: (NORMAL, _w(), {"stale": ("hosts", "db_conn")}),
    # PostgreSQL workload mechanism'leri. Değerler 2026-11-27 lab ölçümlerinin büyüklük sırasındadır (dakika başına).
    "deadlock": lambda: (NORMAL, _w(dbstats={"dl": 1.0}, pg_errors={"svc_checkin": {"dl": 3}, "svc_reservation": {"dl": 2}},
                                    apps={C: {"err": 0.03}}, lock_total=1.0, lock_pairs={(B, C): 1.0},
                                    edges=[_edge(B, C, 8, blocker_wait="Lock"), _edge(C, B, 7, blocker_wait="Lock")]), {}),
    "single_deadlock": lambda: (NORMAL, _w(dbstats={"dl": 0.2}, pg_errors={"svc_checkin": {"dl": 1}}), {}),
    "serialization": lambda: (_w(dbstats={"rb": 70.0}), _w(dbstats={"rb": 226.0, "cm": 8700.0},
                              pg_errors={"svc_flightops": {"sr": 60}, "svc_checkin": {"sr": 53}}), {}),
    "temp_spill": lambda: (NORMAL, _w(dbstats={"tmp": 2.0e9}, apps={B: {"exec_ms": 40000.0}}), {}),
    "bulk_insert": lambda: (NORMAL, _w(pg=_pg(ins=1.0e6, wal=8.0e8, ret=7.0e6), apps={C: {"exec_ms": 20000.0}}), {}),
    "commit_storm": lambda: (NORMAL, _w(pg=_pg(ins=12000.0, wal=4.5e6), dbstats={"cm": 16000.0},
                                        apps={B: {"exec_ms": 9000.0}}), {}),
    # Canlı ölçüm (2026-11-28, D11): exec time artışı source eşiğinin altında, çağrı sayısı ~25 kat
    "commit_storm_cheap": lambda: (NORMAL, _w(pg=_pg(ins=24000.0, wal=8.0e6), dbstats={"cm": 32000.0},
                                              apps={F: {"exec_ms": 460.0, "calls": 16000.0},
                                                    C: {"exec_ms": 470.0, "calls": 15400.0}}), {}),
    "update_churn": lambda: (NORMAL, _w(pg=_pg(writes=150000.0, wal=8.0e7, autovac_blocks=15000.0, ret=1.3e7),
                                        apps={R: {"exec_ms": 30000.0}}), {}),
    "vacuum_blocked": lambda: (NORMAL, _w(pg=_pg(writes=140000.0, wal=6.0e7),
                                          apps={R: {"exec_ms": 25000.0, "idle_tx": 2.0}, C: {"idle_tx": 2.0},
                                                F: {"idle_tx": 2.0}, B: {"idle_tx": 2.0}}), {}),
    "heavy_read": lambda: (NORMAL, _w(pg=_pg(ret=1.5e9, active_ms=300000.0), hosts={"db-vm": 78.0},
                                      apps={C: {"exec_ms": 90000.0, "mean_ms": 60.0, "p95": 95.0}}), {}),
    "db_memory": lambda: (_w(host_mem={"db-vm": 52.0}), _w(host_mem={"db-vm": 58.0}, pg=_pg(active_ms=100000.0),
                                                           apps={R: {"exec_ms": 60000.0}}), {}),
    "idle_in_tx": lambda: (NORMAL, _w(apps={C: {"idle_tx": 5.0}, F: {"idle_tx": 5.0}}), {}),
    "table_lock": lambda: (NORMAL, _w(
        apps={C: {"mean_ms": 700.0, "p95": 2500.0, "pool_a": 7.0, "sessions": 8.0}},
        lock_total=5.0, lock_pairs={(C, R): 4.0, (R, R): 1.0},
        edges=[_edge(C, R, 11, wait_event="relation"), _edge(R, R, 10, wait_event="relation"),
               _edge(C, C, 3, blocker_wait="Lock", wait_event="relation"), _edge(C, R, 1)], pg_lock_logs=40), {}),
    "advisory_lock": lambda: (NORMAL, _w(
        lock_total=4.0, lock_pairs={(C, F): 3.0, (F, F): 1.0},
        edges=[_edge(C, F, 14, wait_event="advisory"), _edge(F, F, 11, wait_event="advisory"),
               _edge(F, C, 7, blocker_wait="Lock", wait_event="advisory")]), {}),
    "point_queries": lambda: (NORMAL, _w(dbstats={"cm": 44000.0}, pg=_pg(ret=1.3e7),
                                         apps={C: {"calls": 9566.0, "exec_ms": 348.0}, F: {"calls": 9860.0, "exec_ms": 303.0}}), {}),
    # Çağrı sayısı arttı ama query'ler ağır (seq scan): query flood değil
    "heavy_calls": lambda: (NORMAL, _w(apps={C: {"calls": 6000.0, "exec_ms": 150000.0}}), {}),
    # Arka plan trafiği arttı ama hiçbir workload mechanism'i yok (rollback, commit ve tuple okuma trafikle artar)
    "busy_traffic": lambda: (_w(dbstats={"rb": 70.0, "cm": 1500.0}, pg=_pg(ret=1.4e6, ins=60.0, wal=2.0e5)),
                             _w(dbstats={"rb": 190.0, "cm": 4700.0}, pg=_pg(ret=5.5e6, ins=180.0, wal=4.0e5, writes=250.0),
                                host_mem={"db-vm": 32.0}, apps={R: {"idle_tx": 1.0}}), {}),
}
