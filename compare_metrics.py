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
    p.add_argument("--out", default="/results/",
                   help="Directory di output per i file prodotti")
    return p.parse_args()


# ── Lettura CSV esterno ───────────────────────────────────────────────────────

def load_external(path: str) -> list[dict]:
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


# ── Lettura CSV interno ───────────────────────────────────────────────────────

def load_internal(path: str) -> list[dict]:
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


# ── Statistiche aggregate canale interno ─────────────────────────────────────

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

    if not ext_rows:
        print("ERRORE: CSV esterno vuoto o non trovato.", file=sys.stderr)
        sys.exit(1)

    # ── Calcola statistiche aggregate ─────────────────────────
    int_stats = compute_internal_stats(int_rows)
    ext_stats = compute_external_stats(ext_rows, duration_s=int_stats.get("duration_s", 0))

    # ── Stampa riepilogo a schermo ────────────────────────────
    print_summary(ext_stats, int_stats)

    # Nome query dal path del CSV (cerca q1/q2 ovunque nel nome file)
    import re as _re
    m = _re.search(r'\b(q[12])\b', Path(args.ext).name)
    query_tag = m.group(1) if m else "q1"

    # ── CSV di confronto per finestra ─────────────────────────
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

    print(f"Summary testuale salvato in: {summary_path}\n")


if __name__ == "__main__":
    main()