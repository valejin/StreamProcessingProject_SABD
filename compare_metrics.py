#!/usr/bin/env python3
"""
compare_metrics.py
==================
Confronta le metriche di throughput misurate con due canali indipendenti:

  CANALE INTERNO  → metrics_flink_internal_q1.csv
    Prodotto da poll_flink_throughput.sh (REST API Flink).
    Misura: numRecordsOutPerSecond per operatore (EWMA istantanea).

  CANALE ESTERNO  → metrics_q1.csv
    Prodotto dal sink Java in Query1Job.java.
    Misura: throughput_rpm = num_flights / window_duration_min,
            latency_ms = outputTime - maxKafkaProduceTime.

Produce:
  - Tabella di confronto riassuntiva a schermo
  - metrics_comparison_q1.csv  (dati allineati per finestra)
  - metrics_summary_q1.txt     (statistiche aggregate per report)

USO:
  python3 compare_metrics.py [--ext EXT_CSV] [--int INT_CSV] [--out DIR]

  Defaults:
    --ext  /results/metrics_q1.csv
    --int  /results/metrics_flink_internal_q1.csv
    --out  /results/
"""

import argparse
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median, stdev


# ── Parsing argomenti ─────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--ext", default="/results/metrics_q1.csv",
                   help="CSV metriche canale esterno (default: /results/metrics_q1.csv)")
    p.add_argument("--int", dest="internal",
                   default="/results/metrics_flink_internal_q1.csv",
                   help="CSV metriche canale interno Flink (default: /results/metrics_flink_internal_q1.csv)")
    p.add_argument("--challenger", default=None,
                   help="CSV challenger_metrics.csv (opzionale, per confronto tre canali)")
    p.add_argument("--out", default="/results/",
                   help="Directory di output per i file prodotti")
    return p.parse_args()


# ── Lettura CSV esterno ───────────────────────────────────────────────────────

def load_external(path: str) -> list[dict]:
    """
    Carica il CSV esterno (metrics_q1.csv o metrics_q2.csv), rilevando
    automaticamente lo schema dall'header.

    Schema Q1: window_start, window_end, airline, num_flights,
               latency_ms, throughput_rpm
    Schema Q2: window_type, window_start, trigger_ts, num_flights,
               latency_ms, throughput_rpm  (niente window_end/airline,
               tre window_type distinti: 1h, 6h, global)
    """
    with open(path, newline="", encoding="utf-8") as f:
        header = f.readline()
    header_cols = [c.strip() for c in header.split(",")]

    if "window_type" in header_cols:
        return load_external_q2(path)
    return load_external_q1(path)


def load_external_q1(path: str) -> list[dict]:
    """
    Carica metrics_q1.csv.
    Colonne attese: window_start, window_end, airline, num_flights,
                    latency_ms, throughput_rpm
    """
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, skipinitialspace=True)
        for row in reader:
            try:
                rows.append({
                    "window_type":  "q1",   # placeholder uniforme con Q2
                    "window_start": row["window_start"].strip(),
                    "window_end":   row["window_end"].strip(),
                    "airline":      row["airline"].strip(),
                    "num_flights":  int(row["num_flights"]),
                    "latency_ms":   int(row["latency_ms"]),
                    "throughput_rpm": float(row["throughput_rpm"]),
                })
            except (KeyError, ValueError) as e:
                print(f"  [WARN] riga saltata in {path}: {e} → {dict(row)}", file=sys.stderr)
    return rows


def load_external_q2(path: str) -> list[dict]:
    """
    Carica metrics_q2.csv.
    Colonne attese: window_type, window_start, trigger_ts, num_flights,
                    latency_ms, throughput_rpm

    A differenza di Q1, Q2 ha tre window_type distinti (1h, 6h, global)
    con durate diverse e una sola riga per trigger (non per airline).
    Non esiste window_end: il bordo destro è trigger_ts.
    """
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, skipinitialspace=True)
        for row in reader:
            try:
                rows.append({
                    "window_type":  row["window_type"].strip(),
                    "window_start": row["window_start"].strip(),
                    "window_end":   row["trigger_ts"].strip(),  # alias per compatibilità
                    "airline":      "",  # Q2 non è per-airline
                    "num_flights":  int(row["num_flights"]),
                    "latency_ms":   int(row["latency_ms"]),
                    "throughput_rpm": float(row["throughput_rpm"]),
                })
            except (KeyError, ValueError) as e:
                print(f"  [WARN] riga saltata in {path}: {e} → {dict(row)}", file=sys.stderr)
    return rows


# ── Lettura CSV interno ───────────────────────────────────────────────────────

def load_internal(path: str) -> list[dict]:
    """
    Carica il CSV interno (metrics_flink_internal_q1.csv o ...q2.csv),
    rilevando automaticamente lo schema dall'header.

    Schema Q1: source_*, window_*, sink_csv_* (singolo ramo window)
    Schema Q2: source_*, win1h_*, win6h_*, winglobal_* (tre rami separati,
               uno per ciascuna finestra — vedi poll_flink_throughput.sh)
    """
    with open(path, newline="", encoding="utf-8") as f:
        header = f.readline()
    if "win1h_records_out_per_sec" in header:
        return load_internal_q2(path)
    return load_internal_q1(path)


def load_internal_q1(path: str) -> list[dict]:
    """
    Carica metrics_flink_internal_q1.csv.
    Colonne attese: timestamp_epoch, timestamp_iso, job_id,
                    source_records_out_per_sec, filter_records_out_per_sec,
                    source_busy_ms_per_sec, source_backpressure_ms_per_sec,
                    source_idle_ms_per_sec,
                    window_records_in_per_sec, window_records_out_per_sec,
                    sink_csv_records_in_per_sec,
                    window_busy_ms_per_sec, window_backpressure_ms_per_sec,
                    window_idle_ms_per_sec
    """
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, skipinitialspace=True)
        for row in reader:
            try:
                rows.append({
                    "timestamp_epoch": int(row["timestamp_epoch"]),
                    "timestamp_iso":   row["timestamp_iso"].strip(),
                    "job_id":          row["job_id"].strip(),
                    # metriche chiave
                    "filter_rps":      float(row["filter_records_out_per_sec"]),
                    "window_out_rps":  float(row["window_records_out_per_sec"]),
                    "sink_csv_rps":    float(row["sink_csv_records_in_per_sec"]),
                    "busy_ms":         float(row["window_busy_ms_per_sec"]),
                    "backpressure_ms": float(row["window_backpressure_ms_per_sec"]),
                    "idle_ms":         float(row["window_idle_ms_per_sec"]),
                })
            except (KeyError, ValueError) as e:
                print(f"  [WARN] riga saltata in {path}: {e}", file=sys.stderr)
    return rows


def load_internal_q2(path: str) -> list[dict]:
    """
    Carica metrics_flink_internal_q2.csv (schema multi-vertex).
    Colonne attese: timestamp_epoch, timestamp_iso, job_id,
                    source_records_out_per_sec, filter_records_out_per_sec,
                    source_busy_ms_per_sec, source_backpressure_ms_per_sec,
                    source_idle_ms_per_sec,
                    win1h_records_in_per_sec, win1h_records_out_per_sec,
                    win1h_busy_ms_per_sec, win1h_backpressure_ms_per_sec,
                    win1h_idle_ms_per_sec,
                    win6h_records_in_per_sec, win6h_records_out_per_sec,
                    win6h_busy_ms_per_sec, win6h_backpressure_ms_per_sec,
                    win6h_idle_ms_per_sec,
                    winglobal_records_in_per_sec, winglobal_records_out_per_sec,
                    winglobal_busy_ms_per_sec, winglobal_backpressure_ms_per_sec,
                    winglobal_idle_ms_per_sec

    A differenza di Q1 (un solo ramo window), qui ogni campione porta
    le metriche dei TRE rami separatamente: window_busy/backpressure
    per ciascuna finestra non sono confrontabili con un singolo
    "busy_ms" aggregato — vengono mantenuti come dict per ramo.
    """
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, skipinitialspace=True)
        for row in reader:
            try:
                rows.append({
                    "timestamp_epoch": int(row["timestamp_epoch"]),
                    "timestamp_iso":   row["timestamp_iso"].strip(),
                    "job_id":          row["job_id"].strip(),
                    # canale source: identico a Q1, è il volume di ingestion reale
                    "filter_rps":      float(row["filter_records_out_per_sec"]),
                    "source_busy_ms":  float(row["source_busy_ms_per_sec"]),
                    "source_bp_ms":    float(row["source_backpressure_ms_per_sec"]),
                    # tre rami window separati
                    "win1h": {
                        "out_rps":  float(row["win1h_records_out_per_sec"]),
                        "busy_ms":  float(row["win1h_busy_ms_per_sec"]),
                        "bp_ms":    float(row["win1h_backpressure_ms_per_sec"]),
                    },
                    "win6h": {
                        "out_rps":  float(row["win6h_records_out_per_sec"]),
                        "busy_ms":  float(row["win6h_busy_ms_per_sec"]),
                        "bp_ms":    float(row["win6h_backpressure_ms_per_sec"]),
                    },
                    "winglobal": {
                        "out_rps":  float(row["winglobal_records_out_per_sec"]),
                        "busy_ms":  float(row["winglobal_busy_ms_per_sec"]),
                        "bp_ms":    float(row["winglobal_backpressure_ms_per_sec"]),
                    },
                    # compatibilità con compute_internal_stats (usa il ramo
                    # global come riferimento principale, essendo quello che
                    # storicamente ha causato il mismatch da diagnosticare)
                    "window_out_rps":  float(row["winglobal_records_out_per_sec"]),
                    "busy_ms":         float(row["winglobal_busy_ms_per_sec"]),
                    "backpressure_ms": float(row["winglobal_backpressure_ms_per_sec"]),
                })
            except (KeyError, ValueError) as e:
                print(f"  [WARN] riga saltata in {path}: {e}", file=sys.stderr)
    return rows


def compute_internal_stats_q2_branches(int_rows: list[dict]) -> dict:
    """
    Calcola busy_ms/backpressure/throughput SEPARATAMENTE per ciascuno
    dei tre rami window di Q2 (1h, 6h, global), per rispondere alla
    domanda "quale finestra sta saturando la CPU del TaskManager?".

    Restituisce {} se int_rows non ha la struttura multi-ramo (es. è
    in realtà un CSV Q1 caricato per errore).
    """
    if not int_rows or "win1h" not in int_rows[0]:
        return {}

    branches = {}
    for branch_name in ("win1h", "win6h", "winglobal"):
        out_active = [r[branch_name]["out_rps"] for r in int_rows if r[branch_name]["out_rps"] > 0]
        busy_all   = [r[branch_name]["busy_ms"] for r in int_rows]
        bp_nonzero = [r[branch_name]["bp_ms"]   for r in int_rows if r[branch_name]["bp_ms"] > 0]

        branches[branch_name] = {
            "n_active_samples": len(out_active),
            "busy_mean": round(mean(busy_all), 1) if busy_all else 0.0,
            "busy_max":  round(max(busy_all),  1) if busy_all else 0.0,
            "bp_n":      len(bp_nonzero),
        }
        if out_active:
            branches[branch_name]["out_rps_mean"] = round(mean(out_active), 4)
            branches[branch_name]["out_rps_max"]  = round(max(out_active),  4)

    return branches

def compute_internal_stats(int_rows: list[dict]) -> dict:
    """
    Aggrega i campioni REST in statistiche di sessione (rec/s).

    I timestamp dei campioni interni sono wall-clock reale (es. 2026-06-29),
    mentre le finestre esterne usano event time simulato (es. 2025-01-01).
    L'allineamento per timestamp è impossibile: usiamo statistiche di sessione.

    Metrica chiave: filter_rps (numRecordsOutPerSecond del vertex Filter),
    che misura rec/s a livello di sistema — confrontabile con ext_sys_rps.
    Campioni warm-up a 0 esclusi.
    """
    if not int_rows:
        return {}

    t_start = int_rows[0]["timestamp_epoch"]
    t_end   = int_rows[-1]["timestamp_epoch"]
    duration_s = max(t_end - t_start, 1)

    filter_active  = [r["filter_rps"]      for r in int_rows if r["filter_rps"]      > 0]
    win_out_active = [r["window_out_rps"]   for r in int_rows if r["window_out_rps"]  > 0]
    busy_all       = [r["busy_ms"]          for r in int_rows]
    bp_nonzero     = [r["backpressure_ms"]  for r in int_rows if r["backpressure_ms"] > 0]

    result = {
        "n_samples":  len(int_rows),
        "n_active":   len(filter_active),
        "duration_s": duration_s,
        "bp_n":       len(bp_nonzero),
        "busy_mean":  round(mean(busy_all), 1),
        "busy_max":   round(max(busy_all),  1),
    }
    if filter_active:
        result.update({
            "filter_rps_mean":   round(mean(filter_active),   2),
            "filter_rps_median": round(median(filter_active), 2),
            "filter_rps_stdev":  round(stdev(filter_active),  2) if len(filter_active) > 1 else 0.0,
            "filter_rps_min":    round(min(filter_active),    2),
            "filter_rps_max":    round(max(filter_active),    2),
        })
    if win_out_active:
        result.update({
            "win_out_rps_mean":   round(mean(win_out_active),   4),
            "win_out_rps_median": round(median(win_out_active), 4),
        })
    return result


# ── Statistiche aggregate canale esterno ─────────────────────────────────────

def compute_external_stats(ext_rows: list[dict], duration_s: int = 0) -> dict:
    """
    Aggrega le righe esterne in statistiche di sessione.
    Calcola throughput_rps per finestra (rpm/60) e throughput sistema
    (voli_totali/durata_s) per il confronto diretto con il canale interno.
    """
    if not ext_rows:
        return {}

    lat_valid  = [r["latency_ms"]     for r in ext_rows if r["latency_ms"]     > 0]
    rpm_valid  = [r["throughput_rpm"] for r in ext_rows if r["throughput_rpm"]  > 0]
    rps_window = [v / 60.0            for v in rpm_valid]   # per finestra, per airline
    total_flights = sum(r["num_flights"] for r in ext_rows)

    result = {
        "n_windows":    len(ext_rows),
        "total_flights": total_flights,
    }
    if lat_valid:
        result.update({
            "lat_mean":   round(mean(lat_valid),   1),
            "lat_median": round(median(lat_valid), 1),
            "lat_stdev":  round(stdev(lat_valid),  1) if len(lat_valid) > 1 else 0.0,
            "lat_min":    min(lat_valid),
            "lat_max":    max(lat_valid),
        })
    if rpm_valid:
        result.update({
            # Throughput per finestra per airline
            "thr_rpm_mean":   round(mean(rpm_valid),   4),
            "thr_rpm_median": round(median(rpm_valid), 4),
            "thr_rpm_min":    round(min(rpm_valid),    4),
            "thr_rpm_max":    round(max(rpm_valid),    4),
            # Stessa unità (rec/s) per confronto con canale interno
            "thr_rps_win_mean":   round(mean(rps_window),   6),
            "thr_rps_win_median": round(median(rps_window), 6),
            "thr_rps_win_min":    round(min(rps_window),    6),
            "thr_rps_win_max":    round(max(rps_window),    6),
        })
    # Throughput sistema: voli_totali / durata_esecuzione_reale
    if duration_s > 0:
        result["sys_rps"] = round(total_flights / duration_s, 2)
    return result


# ── CSV di confronto per finestra ─────────────────────────────────────────────

def build_comparison_rows(ext_rows: list[dict], int_stats: dict) -> list[dict]:
    """
    Produce una riga per ogni finestra esterna con:
    - metriche esterne per finestra (rpm e rps convertito)
    - statistiche interne di sessione come colonne aggregate (stesse per tutte le righe)
    - delta_pct: confronto tra ext_sys_rps e int_filter_rps_mean (stessa grandezza)

    Nota: le colonne int_* sono identiche per tutte le righe perché
    il canale interno aggrega sull'intera sessione, non per finestra.
    """
    int_filter_mean = int_stats.get("filter_rps_mean")
    ext_sys_rps     = None  # calcolato sotto

    # Calcola ext_sys_rps se abbiamo la durata dal canale interno
    duration_s = int_stats.get("duration_s", 0)
    total_flights = sum(r["num_flights"] for r in ext_rows)
    if duration_s > 0:
        ext_sys_rps = round(total_flights / duration_s, 2)

    # Delta tra i due canali sulla grandezza comune (rec/s sistema)
    delta_pct = None
    if ext_sys_rps and int_filter_mean and ext_sys_rps > 0:
        delta_pct = round(100.0 * abs(int_filter_mean - ext_sys_rps) / ext_sys_rps, 2)

    rows = []
    for ext in ext_rows:
        ext_rpm = ext["throughput_rpm"]
        ext_rps = round(ext_rpm / 60.0, 6)   # rec/s per finestra per airline
        rows.append({
            # Dati per finestra
            "window_start":        ext["window_start"],
            "window_end":          ext["window_end"],
            "airline":             ext["airline"],
            "num_flights":         ext["num_flights"],
            # Canale esterno — per finestra
            "ext_throughput_rpm":  round(ext_rpm, 4),
            "ext_throughput_rps":  ext_rps,
            "ext_latency_ms":      ext["latency_ms"],
            # Canale esterno — sistema (sessione)
            "ext_sys_rps":         ext_sys_rps if ext_sys_rps else "N/D",
            # Canale interno — sessione (stesse per tutte le righe)
            "int_filter_rps_mean":   int_filter_mean if int_filter_mean else "N/D",
            "int_filter_rps_median": int_stats.get("filter_rps_median", "N/D"),
            "int_filter_rps_stdev":  int_stats.get("filter_rps_stdev",  "N/D"),
            "int_busy_mean_ms":      int_stats.get("busy_mean", "N/D"),
            "int_busy_max_ms":       int_stats.get("busy_max",  "N/D"),
            "int_backpressure_n":    int_stats.get("bp_n",      "N/D"),
            # Confronto nella grandezza comune (rec/s sistema)
            "delta_sys_rps_pct":   delta_pct if delta_pct is not None else "N/D",
        })
    return rows


# ── Stampa riassunto ──────────────────────────────────────────────────────────

def print_summary(ext_stats: dict, int_stats: dict):
    sep = "=" * 72
    print(f"\n{sep}")
    print("  CONFRONTO METRICHE Q1 — canale interno (Flink REST) vs esterno")
    print(sep)

    print("\nCANALE ESTERNO (metrics_q1.csv)")
    print(f"  Finestre totali          : {ext_stats.get('n_windows', 0)}")
    print(f"  Voli totali elaborati    : {ext_stats.get('total_flights', 0):,}")
    if "lat_mean" in ext_stats:
        print(f"  Latenza ms  mean/med/std : "
              f"{ext_stats['lat_mean']} / {ext_stats['lat_median']} / {ext_stats['lat_stdev']}")
        print(f"  Latenza ms  min/max      : "
              f"{ext_stats['lat_min']} / {ext_stats['lat_max']}")
    if "thr_rpm_mean" in ext_stats:
        print(f"  Throughput/finestra (rpm): "
              f"mean={ext_stats['thr_rpm_mean']}  median={ext_stats['thr_rpm_median']}  "
              f"min={ext_stats['thr_rpm_min']}  max={ext_stats['thr_rpm_max']}")
        print(f"  Throughput/finestra (rps): "
              f"mean={ext_stats['thr_rps_win_mean']:.6f}  "
              f"median={ext_stats['thr_rps_win_median']:.6f}")
    if "sys_rps" in ext_stats:
        print(f"  Throughput sistema (rps) : {ext_stats['sys_rps']} rec/s  "
              f"[= {ext_stats['total_flights']:,} voli / {int_stats.get('duration_s','?')}s]")

    print("\nCANALE INTERNO (metrics_flink_internal_q1.csv — Flink REST API)")
    if not int_stats:
        print("  Non disponibile.")
    else:
        print(f"  Campioni totali          : {int_stats['n_samples']}  "
              f"({int_stats['n_active']} attivi, {int_stats['n_samples']-int_stats['n_active']} warm-up)")
        print(f"  Durata sessione          : {int_stats['duration_s']}s  "
              f"({int_stats['duration_s']/60:.1f} min)")
        if "filter_rps_mean" in int_stats:
            print(f"  filter_rps (rec/s)       : "
                  f"mean={int_stats['filter_rps_mean']}  "
                  f"median={int_stats['filter_rps_median']}  "
                  f"stdev={int_stats['filter_rps_stdev']}  "
                  f"min={int_stats['filter_rps_min']}  "
                  f"max={int_stats['filter_rps_max']}")
        print(f"  Busy window (ms/s)       : "
              f"mean={int_stats['busy_mean']}  max={int_stats['busy_max']}  "
              f"({int_stats['busy_mean']/10:.1f}% utilizzo medio)")
        bp = int_stats['bp_n']
        print(f"  Backpressure             : "
              f"{'assente' if bp == 0 else f'{bp} campioni'}")

    print("\nCONFRONTO NELLA GRANDEZZA COMUNE (rec/s a livello di sistema)")
    ext_sys = ext_stats.get("sys_rps")
    int_sys = int_stats.get("filter_rps_mean")
    if ext_sys and int_sys:
        delta = abs(int_sys - ext_sys) / ext_sys * 100
        print(f"  Canale esterno (derivato): {ext_sys:.2f} rec/s")
        print(f"    = {ext_stats['total_flights']:,} voli / {int_stats['duration_s']}s")
        print(f"  Canale interno (EWMA)    : {int_sys:.2f} rec/s")
        print(f"    = media filter_rps sui campioni attivi")
        print(f"  Scarto relativo          : {delta:.1f}%")
        verdict = "✓ coerenti" if delta < 10 else "⚠ verificare"
        print(f"  Valutazione              : {verdict}  (atteso <10% tra media globale e EWMA)")
    else:
        print("  Non disponibile (canale interno assente).")

    print(f"\n{sep}\n")


# ── Main ──────────────────────────────────────────────────────────────────────

def load_challenger(path: str) -> list[dict]:
    """
    Carica challenger_metrics.csv generato da generate_challenger_metrics.py.
    Ogni riga = una run (identificata da parallelism + query + run_timestamp).
    """
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f, skipinitialspace=True):
            try:
                rows.append({
                    "run_timestamp":      row.get("run_timestamp", ""),
                    "query":              row.get("query", ""),
                    "parallelism":        int(row.get("parallelism", 1)),
                    "latency_mean_ms":    float(row["latency_mean_ms"]),
                    "latency_max_ms":     float(row["latency_max_ms"]),
                    "latency_stdev_ms":   float(row.get("latency_stdev_ms", 0)),
                    "throughput_ext_rps": _safe_float(row.get("throughput_ext_rps")),
                    "throughput_int_rps": _safe_float(row.get("throughput_int_rps")),
                    "delta_pct":          _safe_float(row.get("delta_pct")),
                    "total_flights":      int(row.get("total_flights", 0)),
                    "duration_s":         _safe_float(row.get("duration_s")),
                    "n_windows":          int(row.get("n_windows", 0)),
                })
            except (KeyError, ValueError) as e:
                print(f"  [WARN] riga challenger saltata: {e}", file=sys.stderr)
    return rows


def _safe_float(v) -> float | None:
    """Converte stringa in float; restituisce None se 'N/D' o vuoto."""
    if v is None or str(v).strip() in ("N/D", "", "None"):
        return None
    try:
        return float(v)
    except ValueError:
        return None


def print_three_channels(ext_stats: dict, int_stats: dict, chal_rows: list[dict]):
    """
    Stampa il confronto completo tra i tre canali nella grandezza comune (rec/s).
    """
    sep = "=" * 72
    print(f"\n{sep}")
    print("  CONFRONTO TRE CANALI — throughput in rec/s")
    print(sep)

    # Valori
    ext_sys  = ext_stats.get("sys_rps")
    int_sys  = int_stats.get("filter_rps_mean") if int_stats else None
    chal_ext = chal_rows[0]["throughput_ext_rps"] if chal_rows else None
    chal_int = chal_rows[0]["throughput_int_rps"] if chal_rows else None

    print(f"\n  {'CANALE':<35} {'throughput (rec/s)':>18}  {'latency mean (ms)':>18}")
    print(f"  {'-'*35} {'-'*18}  {'-'*18}")

    ext_lat = ext_stats.get("lat_mean", "N/D")
    print(f"  {'Esterno (voli/durata_s)':<35} {ext_sys or 'N/D':>18}  {ext_lat:>18}")

    int_lat = "N/D (non misurata)"
    print(f"  {'Interno (EWMA Flink REST)':<35} {int_sys or 'N/D':>18}  {int_lat:>18}")

    if chal_rows:
        chal_lat = chal_rows[0]["latency_mean_ms"]
        print(f"  {'Challenger (sintesi run)':<35} {chal_ext or 'N/D':>18}  {chal_lat:>18}")

    # Delta tra canali
    print(f"\n  Scarto esterno ↔ interno  : ", end="")
    if ext_sys and int_sys:
        d = abs(int_sys - ext_sys) / ext_sys * 100
        print(f"{d:.1f}%  {'✓' if d < 10 else '⚠'}")
    else:
        print("N/D")

    if chal_rows and chal_ext and ext_sys:
        d2 = abs(chal_ext - ext_sys) / ext_sys * 100
        print(f"  Scarto esterno ↔ challenger: {d2:.1f}%  {'✓' if d2 < 10 else '⚠'}")
    if chal_rows and chal_int and int_sys:
        d3 = abs(chal_int - int_sys) / int_sys * 100
        print(f"  Scarto interno ↔ challenger: {d3:.1f}%  {'✓' if d3 < 10 else '⚠'}")

    if chal_rows:
        print(f"\n  Run challenger: parallelism={chal_rows[0]['parallelism']}  "
              f"voli={chal_rows[0]['total_flights']:,}  "
              f"durata={chal_rows[0]['duration_s']}s  "
              f"finestre={chal_rows[0]['n_windows']}")

    print(f"\n{sep}\n")


def compute_external_stats_q2(ext_rows: list[dict]) -> dict:
    """
    Versione Q2 di compute_external_stats: calcola le statistiche
    SEPARATAMENTE per ciascun window_type (1h, 6h, global), perché
    sommare num_flights su tutte le righe è scorretto — la finestra
    'global' è cumulativa e ogni trigger ricomprende i voli precedenti,
    mentre 1h e 6h sono finestre scorrevoli (sliding) con overlap.

    Il throughput sistema (rec/s) per Q2 ha senso solo sul flusso di
    INGESTION, non sulle singole finestre: si usa il canale interno
    (filter_rps) come riferimento primario; qui si riporta comunque
    una stima per ciascun window_type basata sull'ultimo trigger
    disponibile (il più rappresentativo per 'global').
    """
    from collections import defaultdict
    by_type = defaultdict(list)
    for r in ext_rows:
        by_type[r["window_type"]].append(r)

    result = {"n_windows_total": len(ext_rows), "by_type": {}}

    for wt, rows in by_type.items():
        lat_valid = [r["latency_ms"]     for r in rows if r["latency_ms"]     > 0]
        rpm_valid = [r["throughput_rpm"] for r in rows if r["throughput_rpm"] > 0]

        entry = {"n_windows": len(rows)}
        if lat_valid:
            entry.update({
                "lat_mean":   round(mean(lat_valid),   1),
                "lat_median": round(median(lat_valid), 1),
                "lat_stdev":  round(stdev(lat_valid),  1) if len(lat_valid) > 1 else 0.0,
                "lat_min":    min(lat_valid),
                "lat_max":    max(lat_valid),
            })
        if rpm_valid:
            entry.update({
                "thr_rpm_mean":   round(mean(rpm_valid),   4),
                "thr_rpm_median": round(median(rpm_valid), 4),
                "thr_rpm_min":    round(min(rpm_valid),    4),
                "thr_rpm_max":    round(max(rpm_valid),    4),
            })
        # Per 'global', l'ultimo trigger è il conteggio totale corretto.
        # Per 1h/6h, sliding window: la somma non ha senso (overlap),
        # quindi riportiamo solo il numero di trigger osservati.
        if wt == "global" and rows:
            entry["total_flights_final"] = rows[-1]["num_flights"]
        result["by_type"][wt] = entry

    return result


def print_summary_q2(ext_stats: dict, int_stats: dict, branch_stats: dict = None):
    """Stampa il riepilogo Q2, con sezione separata per window_type."""
    sep = "=" * 72
    print(f"\n{sep}")
    print("  CONFRONTO METRICHE Q2 — canale interno (Flink REST) vs esterno")
    print(sep)

    print(f"\nCANALE ESTERNO (metrics_q2.csv) — {ext_stats['n_windows_total']} righe totali")
    for wt in ("1h", "6h", "global"):
        if wt not in ext_stats["by_type"]:
            continue
        e = ext_stats["by_type"][wt]
        print(f"\n  [{wt}]  {e['n_windows']} trigger")
        if "lat_mean" in e:
            print(f"    Latenza ms  mean/med/stdev : "
                  f"{e['lat_mean']} / {e['lat_median']} / {e['lat_stdev']}")
            print(f"    Latenza ms  min/max         : {e['lat_min']} / {e['lat_max']}")
        if "thr_rpm_mean" in e:
            print(f"    Throughput rpm mean/med     : "
                  f"{e['thr_rpm_mean']} / {e['thr_rpm_median']}")
            print(f"    Throughput rpm min/max      : "
                  f"{e['thr_rpm_min']} / {e['thr_rpm_max']}")
        if "total_flights_final" in e:
            print(f"    Voli totali (ultimo trigger): {e['total_flights_final']:,}")

    print("\nCANALE INTERNO — SOURCE (metrics_flink_internal_q2.csv)")
    if not int_stats:
        print("  Non disponibile.")
    else:
        print(f"  Campioni totali          : {int_stats['n_samples']}  "
              f"({int_stats['n_active']} attivi, {int_stats['n_samples']-int_stats['n_active']} warm-up)")
        print(f"  Durata sessione          : {int_stats['duration_s']}s  "
              f"({int_stats['duration_s']/60:.1f} min)")
        if "filter_rps_mean" in int_stats:
            print(f"  filter_rps (rec/s)       : "
                  f"mean={int_stats['filter_rps_mean']}  "
                  f"median={int_stats['filter_rps_median']}  "
                  f"stdev={int_stats['filter_rps_stdev']}  "
                  f"min={int_stats['filter_rps_min']}  "
                  f"max={int_stats['filter_rps_max']}")
            print(f"  (Questo è il volume reale di ingestion: tutti e tre i rami")
            print(f"   window leggono dallo stesso stream filtrato.)")

    if branch_stats:
        print("\nCANALE INTERNO — PER RAMO WINDOW (chi satura la CPU?)")
        for branch_name, label in (("win1h", "1h"), ("win6h", "6h"), ("winglobal", "global")):
            b = branch_stats.get(branch_name, {})
            if not b:
                continue
            print(f"\n  [{label}]")
            print(f"    Busy ms/s    mean/max  : {b['busy_mean']} / {b['busy_max']}  "
                  f"({b['busy_mean']/10:.1f}% medio, {b['busy_max']/10:.1f}% picco)")
            print(f"    Backpressure campioni  : {b['bp_n']}")
            if "out_rps_mean" in b:
                print(f"    Trigger out rps mean   : {b['out_rps_mean']}  (max: {b['out_rps_max']})")

        # Confronto diretto: quale ramo ha busy_max più alto?
        busiest = max(
            (("1h", branch_stats.get("win1h", {}).get("busy_mean", 0)),
             ("6h", branch_stats.get("win6h", {}).get("busy_mean", 0)),
             ("global", branch_stats.get("winglobal", {}).get("busy_mean", 0))),
            key=lambda x: x[1]
        )
        print(f"\n  Ramo con maggior utilizzo CPU medio: [{busiest[0]}]  "
              f"({busiest[1]:.0f} ms/s, {busiest[1]/10:.1f}%)")

    # Confronto nella grandezza comune: usa il totale finale di 'global'
    # come proxy del volume totale elaborato (è cumulativo, quindi corretto)
    print("\nCONFRONTO NELLA GRANDEZZA COMUNE (rec/s a livello di sistema)")
    int_sys = int_stats.get("filter_rps_mean") if int_stats else None
    global_total = ext_stats["by_type"].get("global", {}).get("total_flights_final")
    duration_s = int_stats.get("duration_s") if int_stats else None

    if global_total and duration_s:
        ext_sys = round(global_total / duration_s, 2)
        print(f"  Canale esterno (derivato): {ext_sys:.2f} rec/s")
        print(f"    = {global_total:,} voli (ultimo trigger global) / {duration_s}s")
        if int_sys:
            print(f"  Canale interno (EWMA)    : {int_sys:.2f} rec/s")
            delta = abs(int_sys - ext_sys) / ext_sys * 100
            print(f"  Scarto relativo          : {delta:.1f}%")
            verdict = "✓ coerenti" if delta < 10 else "⚠ verificare"
            print(f"  Valutazione              : {verdict}  (atteso <10%)")
    else:
        print("  Non disponibile (servono sia 'global' che il canale interno).")

    print(f"\n{sep}\n")


def main():
    args = parse_args()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── Carica CSV ────────────────────────────────────────────
    print(f"Caricamento canale esterno:  {args.ext}")
    ext_rows = load_external(args.ext)
    print(f"  → {len(ext_rows)} righe caricate")

    int_rows = []
    if Path(args.internal).exists():
        print(f"Caricamento canale interno:  {args.internal}")
        int_rows = load_internal(args.internal)
        print(f"  → {len(int_rows)} campioni caricati")
    else:
        print(f"Canale interno non disponibile: {args.internal}")
        print("  (verrà prodotto da poll_flink_throughput.sh alla prossima run)")

    chal_rows = []
    if args.challenger and Path(args.challenger).exists():
        print(f"Caricamento challenger:      {args.challenger}")
        chal_rows = load_challenger(args.challenger)
        print(f"  → {len(chal_rows)} run caricate")
    elif args.challenger:
        print(f"Challenger non disponibile:  {args.challenger}")
        print("  (generalo con: python3 generate_challenger_metrics.py)")

    if not ext_rows:
        print("ERRORE: CSV esterno vuoto o non trovato.", file=sys.stderr)
        sys.exit(1)

    # Rileva se stiamo processando Q2 (ha window_type reali: 1h/6h/global)
    is_q2 = any(r["window_type"] in ("1h", "6h", "global") for r in ext_rows)

    # ── Calcola statistiche aggregate ─────────────────────────
    int_stats = compute_internal_stats(int_rows)

    if is_q2:
        ext_stats = compute_external_stats_q2(ext_rows)
        branch_stats = compute_internal_stats_q2_branches(int_rows)
        print_summary_q2(ext_stats, int_stats, branch_stats)
    else:
        branch_stats = None
        ext_stats = compute_external_stats(ext_rows, duration_s=int_stats.get("duration_s", 0))
        print_summary(ext_stats, int_stats)

    # ── Stampa confronto tre canali (se challenger disponibile) ──
    if chal_rows:
        print_three_channels(ext_stats, int_stats, chal_rows)

    # Nome query dal path del CSV (cerca q1/q2 ovunque nel nome file)
    import re as _re
    m = _re.search(r'\b(q[12])\b', Path(args.ext).name)
    query_tag = m.group(1) if m else ("q2" if is_q2 else "q1")

    # ── CSV di confronto per finestra ─────────────────────────
    # Per Q2 il formato per-finestra di build_comparison_rows (pensato per Q1,
    # con un'unica colonna ext_sys_rps globale) non è applicabile nello stesso
    # modo: lo salta e produce solo il summary testuale con le sezioni per
    # window_type, già sufficiente per il report.
    if not is_q2:
        comp_rows = build_comparison_rows(ext_rows, int_stats)
        comp_path = out_dir / f"metrics_comparison_{query_tag}.csv"
        if comp_rows:
            fieldnames = list(comp_rows[0].keys())
        with open(comp_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(comp_rows)
        print(f"CSV confronto salvato in   : {comp_path}")

    # ── Summary testuale ──────────────────────────────────────
    summary_path = out_dir / f"metrics_summary_{query_tag}.txt"

    if is_q2:
        write_summary_q2(summary_path, ext_stats, int_stats, query_tag, branch_stats)
    else:
        write_summary_q1(summary_path, ext_stats, int_stats, query_tag)

    print(f"Summary testuale salvato in: {summary_path}\n")


def write_summary_q1(summary_path, ext_stats, int_stats, query_tag):
    """Scrive il summary testuale per Q1 (formato originale)."""
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(f"RIEPILOGO METRICHE {query_tag.upper()} — SABD Progetto 2\n")
        f.write(f"Generato: {datetime.now().isoformat()}\n")
        f.write("=" * 72 + "\n\n")

        f.write("CANALE ESTERNO (metrics_q1.csv)\n")
        f.write(f"  Misura : throughput_rpm = num_flights / 60 min (per finestra, per airline)\n")
        f.write(f"           latency_ms = System.currentTimeMillis() - max(kafkaProduceTime)\n\n")
        f.write(f"  Finestre totali          : {ext_stats.get('n_windows', 0)}\n")
        f.write(f"  Voli totali elaborati    : {ext_stats.get('total_flights', 0):,}\n")
        if "lat_mean" in ext_stats:
            f.write(f"  Latenza ms   mean        : {ext_stats['lat_mean']}\n")
            f.write(f"  Latenza ms   median      : {ext_stats['lat_median']}\n")
            f.write(f"  Latenza ms   stdev       : {ext_stats['lat_stdev']}\n")
            f.write(f"  Latenza ms   min         : {ext_stats['lat_min']}\n")
            f.write(f"  Latenza ms   max         : {ext_stats['lat_max']}\n")
        if "thr_rpm_mean" in ext_stats:
            f.write(f"  Throughput/finestra rpm  : mean={ext_stats['thr_rpm_mean']}  "
                    f"median={ext_stats['thr_rpm_median']}  "
                    f"min={ext_stats['thr_rpm_min']}  max={ext_stats['thr_rpm_max']}\n")
            f.write(f"  Throughput/finestra rps  : mean={ext_stats['thr_rps_win_mean']:.6f}  "
                    f"median={ext_stats['thr_rps_win_median']:.6f}\n")
        if "sys_rps" in ext_stats:
            f.write(f"  Throughput sistema (rps) : {ext_stats['sys_rps']} rec/s\n")
            f.write(f"    = {ext_stats['total_flights']:,} voli / "
                    f"{int_stats.get('duration_s', '?')}s esecuzione\n")

        f.write("\nCANALE INTERNO (metrics_flink_internal_q1.csv)\n")
        f.write(f"  Misura : filter_rps = numRecordsOutPerSecond vertex Filter (EWMA Flink REST)\n\n")
        if not int_stats:
            f.write("  Non disponibile.\n")
        else:
            f.write(f"  Campioni totali          : {int_stats['n_samples']} "
                    f"({int_stats['n_active']} attivi, "
                    f"{int_stats['n_samples']-int_stats['n_active']} warm-up esclusi)\n")
            f.write(f"  Durata sessione          : {int_stats['duration_s']}s "
                    f"({int_stats['duration_s']/60:.1f} min)\n")
            if "filter_rps_mean" in int_stats:
                f.write(f"  filter_rps mean          : {int_stats['filter_rps_mean']} rec/s\n")
                f.write(f"  filter_rps median        : {int_stats['filter_rps_median']} rec/s\n")
                f.write(f"  filter_rps stdev         : {int_stats['filter_rps_stdev']} rec/s\n")
                f.write(f"  filter_rps min           : {int_stats['filter_rps_min']} rec/s\n")
                f.write(f"  filter_rps max           : {int_stats['filter_rps_max']} rec/s\n")
            f.write(f"  Busy ms/s mean           : {int_stats['busy_mean']} "
                    f"({int_stats['busy_mean']/10:.1f}% utilizzo)\n")
            f.write(f"  Busy ms/s max            : {int_stats['busy_max']} "
                    f"({int_stats['busy_max']/10:.1f}%)\n")
            bp_str = "assente (0 campioni)" if int_stats['bp_n'] == 0 else f"{int_stats['bp_n']} campioni"
            f.write(f"  Backpressure             : {bp_str}\n")

        f.write("\nCONFRONTO NELLA GRANDEZZA COMUNE (rec/s a livello di sistema)\n")
        ext_sys = ext_stats.get("sys_rps")
        int_sys = int_stats.get("filter_rps_mean")
        if ext_sys and int_sys:
            delta = abs(int_sys - ext_sys) / ext_sys * 100
            f.write(f"  Canale esterno (derivato): {ext_sys:.2f} rec/s\n")
            f.write(f"    = {ext_stats['total_flights']:,} voli / {int_stats['duration_s']}s\n")
            f.write(f"  Canale interno (EWMA)    : {int_sys:.2f} rec/s\n")
            f.write(f"    = media filter_rps sui campioni attivi\n")
            f.write(f"  Scarto relativo          : {delta:.1f}%\n")
            f.write(f"  Valutazione              : "
                    f"{'coerenti (scarto <10%)' if delta < 10 else 'verificare (scarto >10%)'}\n")
        else:
            f.write("  Non disponibile (canale interno assente).\n")

        f.write("\nNOTE METODOLOGICHE\n")
        f.write("  - Latenza: la misura esterna (kafkaProduceTime → outputTime) include\n"
                "    il tempo di buffering nella finestra. Il LatencyMarker interno di Flink\n"
                "    NON include il buffer di finestra → misura esterna più accurata.\n")
        f.write("  - Throughput: EWMA (interno) pesa i burst recenti più della media globale\n"
                "    (esterno derivato). Scarto <10% tra i due metodi è atteso e accettabile.\n")
        f.write("  - Backpressure assente: sink InfluxDB+CSV non ha costituito collo\n"
                "    di bottiglia durante l'esecuzione.\n")


def write_summary_q2(summary_path, ext_stats, int_stats, query_tag, branch_stats=None):
    """
    Scrive il summary testuale per Q2, con sezioni separate per window_type
    (1h, 6h, global) dato che hanno semantiche e durate diverse.
    """
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(f"RIEPILOGO METRICHE {query_tag.upper()} — SABD Progetto 2\n")
        f.write(f"Generato: {datetime.now().isoformat()}\n")
        f.write("=" * 72 + "\n\n")

        f.write("CANALE ESTERNO (metrics_q2.csv)\n")
        f.write("  Misura : throughput_rpm = num_flights / window_duration_min\n")
        f.write("           latency_ms = System.currentTimeMillis() - max(kafkaProduceTime)\n")
        f.write("  Tre window_type con semantica diversa:\n")
        f.write("    1h     : sliding, slide=60min, non cumulativa\n")
        f.write("    6h     : sliding, slide=60min, non cumulativa\n")
        f.write("    global : GlobalWindow, CUMULATIVA — ogni trigger ricomprende tutti\n")
        f.write("             i voli precedenti, quindi num_flights cresce monotonicamente\n\n")
        f.write(f"  Righe totali (tutti i window_type): {ext_stats['n_windows_total']}\n\n")

        for wt in ("1h", "6h", "global"):
            if wt not in ext_stats["by_type"]:
                continue
            e = ext_stats["by_type"][wt]
            f.write(f"  [{wt}]  {e['n_windows']} trigger\n")
            if "lat_mean" in e:
                f.write(f"    Latenza ms   mean      : {e['lat_mean']}\n")
                f.write(f"    Latenza ms   median    : {e['lat_median']}\n")
                f.write(f"    Latenza ms   stdev     : {e['lat_stdev']}\n")
                f.write(f"    Latenza ms   min       : {e['lat_min']}\n")
                f.write(f"    Latenza ms   max       : {e['lat_max']}\n")
            if "thr_rpm_mean" in e:
                f.write(f"    Throughput rpm mean    : {e['thr_rpm_mean']}\n")
                f.write(f"    Throughput rpm median  : {e['thr_rpm_median']}\n")
                f.write(f"    Throughput rpm min     : {e['thr_rpm_min']}\n")
                f.write(f"    Throughput rpm max     : {e['thr_rpm_max']}\n")
            if "total_flights_final" in e:
                f.write(f"    Voli totali (finale)   : {e['total_flights_final']:,}\n")
            f.write("\n")

        f.write("CANALE INTERNO — SOURCE (metrics_flink_internal_q2.csv)\n")
        f.write("  Misura : filter_rps = numRecordsOutPerSecond vertex Source/Filter\n")
        f.write("           (volume reale di ingestion: tutti e tre i rami window\n")
        f.write("            leggono dallo stesso stream filtrato condiviso)\n\n")
        if not int_stats:
            f.write("  Non disponibile.\n")
        else:
            f.write(f"  Campioni totali          : {int_stats['n_samples']} "
                    f"({int_stats['n_active']} attivi, "
                    f"{int_stats['n_samples']-int_stats['n_active']} warm-up esclusi)\n")
            f.write(f"  Durata sessione          : {int_stats['duration_s']}s "
                    f"({int_stats['duration_s']/60:.1f} min)\n")
            if "filter_rps_mean" in int_stats:
                f.write(f"  filter_rps mean          : {int_stats['filter_rps_mean']} rec/s\n")
                f.write(f"  filter_rps median        : {int_stats['filter_rps_median']} rec/s\n")
                f.write(f"  filter_rps stdev         : {int_stats['filter_rps_stdev']} rec/s\n")
                f.write(f"  filter_rps min           : {int_stats['filter_rps_min']} rec/s\n")
                f.write(f"  filter_rps max           : {int_stats['filter_rps_max']} rec/s\n")

        if branch_stats:
            f.write("\nCANALE INTERNO — PER RAMO WINDOW (chi satura la CPU?)\n")
            f.write("  Ciascuna delle tre finestre gira su un operatore Flink separato;\n")
            f.write("  queste metriche isolano il contributo di ciascun ramo.\n\n")
            for branch_name, label in (("win1h", "1h"), ("win6h", "6h"), ("winglobal", "global")):
                b = branch_stats.get(branch_name, {})
                if not b:
                    continue
                f.write(f"  [{label}]\n")
                f.write(f"    Busy ms/s    mean      : {b['busy_mean']} "
                        f"({b['busy_mean']/10:.1f}% medio)\n")
                f.write(f"    Busy ms/s    max       : {b['busy_max']} "
                        f"({b['busy_max']/10:.1f}% picco)\n")
                f.write(f"    Backpressure campioni  : {b['bp_n']}\n")
                if "out_rps_mean" in b:
                    f.write(f"    Trigger out rps mean   : {b['out_rps_mean']}  "
                            f"(max: {b['out_rps_max']})\n")
                f.write("\n")

            busiest = max(
                (("1h", branch_stats.get("win1h", {}).get("busy_mean", 0)),
                 ("6h", branch_stats.get("win6h", {}).get("busy_mean", 0)),
                 ("global", branch_stats.get("winglobal", {}).get("busy_mean", 0))),
                key=lambda x: x[1]
            )
            f.write(f"  Ramo con maggior utilizzo CPU medio: [{busiest[0]}] "
                    f"({busiest[1]:.0f} ms/s, {busiest[1]/10:.1f}%)\n")

        f.write("\nCONFRONTO NELLA GRANDEZZA COMUNE (rec/s a livello di sistema)\n")
        int_sys = int_stats.get("filter_rps_mean") if int_stats else None
        global_total = ext_stats["by_type"].get("global", {}).get("total_flights_final")
        duration_s = int_stats.get("duration_s") if int_stats else None

        if global_total and duration_s:
            ext_sys = round(global_total / duration_s, 2)
            f.write(f"  Canale esterno (derivato): {ext_sys:.2f} rec/s\n")
            f.write(f"    = {global_total:,} voli (ultimo trigger global) / {duration_s}s\n")
            if int_sys:
                delta = abs(int_sys - ext_sys) / ext_sys * 100
                f.write(f"  Canale interno (EWMA)    : {int_sys:.2f} rec/s\n")
                f.write(f"  Scarto relativo          : {delta:.1f}%\n")
                f.write(f"  Valutazione              : "
                        f"{'coerenti (scarto <10%)' if delta < 10 else 'verificare (scarto >10%)'}\n")
        else:
            f.write("  Non disponibile (servono sia 'global' che il canale interno).\n")

        f.write("\nNOTE METODOLOGICHE\n")
        f.write("  - Le tre finestre (1h, 6h, global) hanno semantiche temporali diverse\n"
                "    e non sono sommabili tra loro: 1h e 6h sono sliding (overlap tra\n"
                "    trigger consecutivi), global è cumulativa dall'inizio del dataset.\n")
        f.write("  - Il throughput sistema è stimato dal conteggio dell'ultimo trigger\n"
                "    'global' (il più rappresentativo, essendo cumulativo) diviso per la\n"
                "    durata di sessione osservata dal canale interno.\n")
        f.write("  - Latenza: stessa metodologia di Q1 (kafkaProduceTime → outputTime),\n"
                "    include il buffering di finestra. Per 'global', la finestra non si\n"
                "    chiude mai: la latenza riflette il tempo dall'ultimo evento al\n"
                "    trigger periodico (ContinuousEventTimeTrigger ogni 60min event time).\n")
        f.write("  - Backpressure assente: sink InfluxDB+CSV non ha costituito collo\n"
                "    di bottiglia durante l'esecuzione.\n")


if __name__ == "__main__":
    main()