#!/usr/bin/env python3
"""SRE Agent kör değerlendirmesi için chaos v2 deneylerini koşar ve Dynatrace analiz pencerelerini üretir.

APP_VM'de çalıştırın (kimlik bilgileri app-vm/.env'den okunur, ekrana basılmaz):
  python3 scripts/chaos-agent-eval-runs.py [--only C0,S03] [--out /opt/airlab/deploy/evidence/chaos-v2-agent-eval]
  python3 scripts/chaos-agent-eval-runs.py --set validation --out /opt/airlab/deploy/evidence/chaos-v2-agent-validation
  python3 scripts/chaos-agent-eval-runs.py --set db --out /opt/airlab/deploy/evidence/chaos-db-agent-eval

Çıktı:
  <out>/windows.yaml       YALNIZ zaman aralıkları (case adı → baseline_from/to, from/to). Agent'a girdi olarak giden
                           tek şey budur; senaryo/initiator bilgisi içermez.
  <out>/runs.json          deney kimlikleri, chaos controller'ın kendi ölçtüğü sonuçlar (karşılaştırma için)
  <out>/<sid>-<id>.json|md deney kaydı ve raporu

Pencereler deneyin BASELINING ve RUNNING fazlarının içine tam dakikaya yuvarlanır (Dynatrace 1 dk çözünürlük).
Her case öncesi/sonrası VM saati harici HTTP Date başlığıyla karşılaştırılır; sapma > 30 sn ise case geçersizdir
(Hyper-V host uykusunda VM saati saatlerce geri kalabiliyor).
"""
from __future__ import annotations

import argparse
import email.utils
import importlib.util
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location("chaos_e2e", os.path.join(HERE, "chaos-e2e.py"))
e2e = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(e2e)

# case → (senaryo, intensity, initiator, targetMode, targets). C0 = chaos yok (yalnız sürekli trafik) kontrol penceresi.
# Tüm senaryolar yalnız PostgreSQL yükü üretir. S03, S04, S10 ve S17 2026-11-24 koşusuyla aynı yük tipidir;
# S01, S11 ve S13 yeni yük tipleridir (table lock, commit storm, advisory lock) → eval/chaos_v2_labels.yaml'daki
# eski etiketleri bu case'ler için geçersizdir. Güncel yük tipleri için --set db ve eval/chaos_db_labels.yaml kullanın.
CASES = {
    "C0": None,
    "S03": ("S03", "MEDIUM", "flightops", "AUTO", []),
    "S04": ("S04", "MEDIUM", "baggage", "SINGLE", []),
    "S01": ("S01", "MEDIUM", "checkin", "MULTI", ["reservation"]),
    "S10": ("S10", "MEDIUM", "flightops", "MULTI", ["baggage"]),
    "S11": ("S11", "HIGH", "baggage", "MULTI", ["reservation"]),
    "S13": ("S13", "HIGH", "checkin", "MULTI", ["flightops"]),
    "S17": ("S17", "HIGH", "baggage", "AUTO", []),
}
# Doğrulama seti (--set validation): aynı yük tipleri farklı initiator / intensity ile, artı ilk sette olmayan tipler.
VALIDATION = {
    "V0": None,
    "V10": ("S10", "HIGH", "reservation", "MULTI", ["checkin"]),
    "V11": ("S11", "HIGH", "reservation", "MULTI", ["flightops"]),
    "V13": ("S13", "HIGH", "flightops", "MULTI", ["baggage"]),
    "V14": ("S14", "HIGH", "checkin", "MULTI", ["baggage"]),
    "V03": ("S03", "HIGH", "flightops", "AUTO", []),
    "V19": ("S19", "MEDIUM", "reservation", "AUTO", []),
}
# PostgreSQL yük seti (--set db): S01–S19'un her biri bir kez. Etiketler: sre-agent/eval/chaos_db_labels.yaml.
# Initiator / intensity, mechanism eşikleri seçilirken kullanılan koşulardan bilerek farklıdır. S20 (rastgele birleşik
# yük) yoktur: beklenen sonucu tanımsızdır.
DB = {
    "D0": None,
    "D01": ("S01", "HIGH", "flightops", "MULTI", ["baggage"]),
    "D02": ("S02", "MEDIUM", "baggage", "MULTI", ["reservation"]),
    "D03": ("S03", "MEDIUM", "checkin", "AUTO", []),
    "D04": ("S04", "HIGH", "reservation", "SINGLE", []),
    "D05": ("S05", "MEDIUM", "flightops", "SINGLE", []),
    "D06": ("S06", "MEDIUM", "baggage", "SINGLE", []),
    "D07": ("S07", "HIGH", "reservation", "SINGLE", []),
    "D08": ("S08", "HIGH", "baggage", "MULTI", ["flightops"]),
    "D09": ("S09", "HIGH", "reservation", "MULTI", ["flightops"]),
    # HIGH: MEDIUM hedefi (%55 connection doluluğu) agent'ın göreli eşiğinin (%60) altında kalır (2026-11-28 koşusu);
    # %55 doluluk bir baskı sayılmadığı için eşik değil case yoğunluğu değiştirildi.
    "D10": ("S10", "HIGH", "checkin", "MULTI", ["reservation"]),
    "D11": ("S11", "MEDIUM", "flightops", "MULTI", ["checkin"]),
    "D12": ("S12", "HIGH", "baggage", "SINGLE", []),
    "D13": ("S13", "MEDIUM", "baggage", "MULTI", ["reservation"]),
    "D14": ("S14", "HIGH", "reservation", "MULTI", ["baggage"]),
    "D15": ("S15", "MEDIUM", "flightops", "MULTI", ["checkin"]),
    "D16": ("S16", "HIGH", "baggage", "MULTI", ["reservation"]),
    "D17": ("S17", "HIGH", "checkin", "AUTO", []),
    "D18": ("S18", "HIGH", "flightops", "MULTI", ["baggage"]),
    "D19": ("S19", "MEDIUM", "checkin", "AUTO", []),
}
DURATIONS = {"baselineSec": 210, "rampSec": 30, "durationSec": 270, "recoverySec": 90, "maxDurationSec": 0}
CONTROL_SEC = 8 * 60
GAP_SEC = 90
MAX_SKEW_SEC = 30


def clock_skew() -> float | None:
    """VM saati - harici saat (sn). Ölçülemezse None."""
    for url in ("https://www.google.com", "https://www.cloudflare.com"):
        try:
            req = urllib.request.Request(url, method="HEAD")
            t0 = time.time()
            with urllib.request.urlopen(req, timeout=10) as r:
                d = r.headers.get("Date")
            t1 = time.time()
            ext = email.utils.parsedate_to_datetime(d).timestamp()
            return round((t0 + t1) / 2 - ext, 1)
        except Exception:  # noqa: BLE001 — bir sonraki kaynağı dene
            continue
    return None


def ceil_min(t: datetime) -> datetime:
    f = t.replace(second=0, microsecond=0)
    return f if f == t else f + timedelta(minutes=1)


def floor_min(t: datetime) -> datetime:
    return t.replace(second=0, microsecond=0)


def iso(t: datetime) -> str:
    return t.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse(s: str) -> datetime:
    s = s.rstrip("Z")
    if "." in s:
        head, frac = s.split(".")
        s = f"{head}.{frac[:6]}"
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


def phase_windows(exp: dict) -> dict | None:
    at = {}
    for tr in exp.get("transitions", []):
        at.setdefault(tr["state"], parse(tr["at"]))
    need = ("BASELINING", "RAMPING_UP", "RUNNING", "RECOVERING")
    if not all(k in at for k in need):
        return None
    bf, bt = ceil_min(at["BASELINING"]), floor_min(at["RAMPING_UP"])
    wf, wt = ceil_min(at["RUNNING"]), floor_min(at["RECOVERING"])
    if (bt - bf) < timedelta(minutes=2) or (wt - wf) < timedelta(minutes=3):
        return None
    return {"baseline_from": iso(bf), "baseline_to": iso(bt), "from": iso(wf), "to": iso(wt)}


def write_yaml(path: str, wins: dict) -> None:
    lines = ["# Chaos v2 agent eval pencereleri — YALNIZ GİRDİ (zaman aralıkları). Üreten: scripts/chaos-agent-eval-runs.py"]
    for k, w in wins.items():
        lines.append(f'{k}: {{baseline_from: "{w["baseline_from"]}", baseline_to: "{w["baseline_to"]}", '
                     f'from: "{w["from"]}", to: "{w["to"]}"}}')
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default="/opt/airlab/app-vm/.env")
    ap.add_argument("--base", default="http://172.20.229.26:8080")
    ap.add_argument("--out", default="/opt/airlab/deploy/evidence/chaos-v2-agent-eval")
    ap.add_argument("--only", default="")
    ap.add_argument("--set", choices=["eval", "validation", "db"], default="eval")
    a = ap.parse_args()
    cases = {"validation": VALIDATION, "db": DB}.get(a.set, CASES)
    e = e2e.env(a.env)
    api = e2e.Api(a.base, e.get("CHAOS_USER", "chaos"), e["CHAOS_PASSWORD"])
    os.makedirs(a.out, exist_ok=True)
    only = [s.strip() for s in a.only.split(",") if s.strip()]
    wins_path, runs_path = os.path.join(a.out, "windows.yaml"), os.path.join(a.out, "runs.json")
    runs = json.load(open(runs_path, encoding="utf-8")) if os.path.exists(runs_path) else {}
    wins = {k: v["windows"] for k, v in runs.items() if v.get("windows") and v.get("valid")}

    for case, spec in cases.items():
        if only and case not in only:
            continue
        e2e.wait_idle(api)
        sk0 = clock_skew()
        print(f"[{case}] başlıyor; VM saat sapması {sk0} sn", flush=True)
        rec: dict = {"case": case, "skew_before": sk0}
        if spec is None:
            t0 = datetime.now(timezone.utc)
            time.sleep(CONTROL_SEC)
            bf = ceil_min(t0)
            rec["windows"] = {"baseline_from": iso(bf), "baseline_to": iso(bf + timedelta(minutes=4)),
                              "from": iso(bf + timedelta(minutes=4)), "to": iso(bf + timedelta(minutes=7))}
            rec["chaos"] = {"note": "chaos yok; yalnız sürekli trafik"}
        else:
            sid, inten, init, mode, tg = spec
            summ = e2e.run_one(api, sid, inten, init, mode, tg, DURATIONS, a.out, f"agent-eval {case}")
            rec["chaos"] = summ
            path = os.path.join(a.out, f"{sid}-{summ.get('id')}.json")
            exp = json.load(open(path, encoding="utf-8")) if os.path.exists(path) else {}
            rec["windows"] = phase_windows(exp)
            rec["experiment_state"] = exp.get("state")
        sk1 = clock_skew()
        rec["skew_after"] = sk1
        rec["valid"] = (rec["windows"] is not None and sk0 is not None and sk1 is not None
                        and abs(sk0) <= MAX_SKEW_SEC and abs(sk1) <= MAX_SKEW_SEC
                        and rec.get("experiment_state", "COMPLETED") == "COMPLETED")
        print(f"[{case}] pencereler={rec['windows']} sapma sonrası={sk1} geçerli={rec['valid']}", flush=True)
        runs[case] = rec
        if rec["valid"]:
            wins[case] = rec["windows"]
        with open(runs_path, "w", encoding="utf-8") as f:
            json.dump(runs, f, ensure_ascii=False, indent=1)
        write_yaml(wins_path, wins)
        time.sleep(GAP_SEC)

    print("\nÖZET")
    for k, r in runs.items():
        c = r.get("chaos") or {}
        print(f"  {k}: geçerli={r['valid']} verdict={c.get('verdict')} affected={c.get('observedAffected')} "
              f"sapma={r.get('skew_before')}/{r.get('skew_after')}")
    return 0 if all(r["valid"] for r in runs.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
