#!/usr/bin/env bash
# =============================================================================
# repeat_run.sh — Ripete N volte la STESSA configurazione (query + parallelism)
#
# Qui il parallelism resta fisso e si ripete più volte la stessa run — serve a
# stimare quanta variabilità c'è a parità di configurazione (cache OS, jitter
# di sistema, Docker, ecc.), per poter dire con più sicurezza se una
# differenza osservata tra parallelism diversi è un segnale reale o rientra
# nel rumore di misura.
#
# Uso:
#   bash benchmark/repeat_run.sh q1 4            # 3 ripetizioni di default
#   bash benchmark/repeat_run.sh q1 4 5          # 5 ripetizioni
#   TIME_SCALE_FACTOR=86400 bash benchmark/repeat_run.sh q1 1
#
# Argomenti:
#   $1  query: q1 | q2                      (obbligatorio)
#   $2  parallelism (intero)                (obbligatorio)
#   $3  numero di ripetizioni                (opzionale, default 3)
#
# Output:
#   Results/repeat_<query>_p<N>_tsf<TSF>/
#     run1/ run2/ run3/ ...     → CSV + summary completi di ciascuna ripetizione
#     aggregate_summary_tm2.txt     → media, deviazione standard e coefficiente di
#                                 variazione (stdev/mean %) delle metriche
#                                 chiave sulle N ripetizioni
#
# Riusa run_pipeline.sh così com'è (nessuna duplicazione di logica): cancella i
# file della query da Results/ prima di ogni run per evitare che un run fallito
# lasci dati vecchi da archiviare per errore.
# =============================================================================

set -euo pipefail

RED='\033[0;31m'; GRN='\033[0;32m'; YLW='\033[1;33m'
BLU='\033[0;34m'; CYN='\033[0;36m'; RST='\033[0m'
log()  { echo -e "${BLU}[$(date +%H:%M:%S)]${RST} $*"; }
ok()   { echo -e "${GRN}[$(date +%H:%M:%S)] ✓ $*${RST}"; }
warn() { echo -e "${YLW}[$(date +%H:%M:%S)] ⚠ $*${RST}"; }
die()  { echo -e "${RED}[$(date +%H:%M:%S)] ✗ ERRORE: $*${RST}" >&2; exit 1; }
sep()  { echo -e "${CYN}$(printf '─%.0s' {1..72})${RST}"; }

QUERY="${1:-}"
PARALLELISM="${2:-}"
N_REPEATS="${3:-3}"

[[ "$QUERY" =~ ^(q1|q2)$ ]] \
    || { echo "Uso: bash repeat_run.sh [q1|q2] <parallelism> [n_repeats=3]"; exit 1; }
[[ "$PARALLELISM" =~ ^[0-9]+$ ]] \
    || { echo "Uso: bash repeat_run.sh [q1|q2] <parallelism> [n_repeats=3]  (parallelism deve essere un intero)"; exit 1; }
[[ "$N_REPEATS" =~ ^[0-9]+$ ]] && (( N_REPEATS >= 1 )) \
    || { echo "n_repeats deve essere un intero >= 1"; exit 1; }

TIME_SCALE_FACTOR="${TIME_SCALE_FACTOR:-86400}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RUN_PIPELINE="$PROJECT_ROOT/run_pipeline.sh"
RESULTS_DIR="$PROJECT_ROOT/Results"

[[ -f "$RUN_PIPELINE" ]] || die "run_pipeline.sh non trovato in $RUN_PIPELINE"

EXPERIMENT_DIR="$RESULTS_DIR/repeat_${QUERY}_p${PARALLELISM}_tsf${TIME_SCALE_FACTOR}"
mkdir -p "$EXPERIMENT_DIR"

sep
echo -e "${CYN}  Repeat test — stessa configurazione ripetuta N volte — [${QUERY^^}]${RST}"
echo "  Parallelism         : $PARALLELISM  (fisso su tutte le ripetizioni)"
echo "  Ripetizioni          : $N_REPEATS"
echo "  TIME_SCALE_FACTOR    : $TIME_SCALE_FACTOR"
echo "  Output finale in     : $EXPERIMENT_DIR"
sep

for i in $(seq 1 "$N_REPEATS"); do
    sep
    log "Ripetizione ${i}/${N_REPEATS} — query=${QUERY} parallelism=${PARALLELISM} tsf=${TIME_SCALE_FACTOR}"
    sep

    # Rimuove i file di questa query rimasti in Results/ da run precedenti, così un fallimento nella
    # rigenerazione viene segnalato esplicitamente invece di archiviare dati vecchi senza dirlo.
    rm -f "$RESULTS_DIR"/*"${QUERY}"*.csv "$RESULTS_DIR"/*"${QUERY}"*.txt 2>/dev/null || true

    TIME_SCALE_FACTOR="$TIME_SCALE_FACTOR" bash "$RUN_PIPELINE" "$QUERY" "$PARALLELISM" \
        || die "run_pipeline.sh fallito alla ripetizione $i — controlla l'output sopra."

    SRC_ARCHIVE="$RESULTS_DIR/${QUERY}_p${PARALLELISM}_tsf${TIME_SCALE_FACTOR}"
    DST_DIR="$EXPERIMENT_DIR/run${i}"

    if [[ -d "$SRC_ARCHIVE" ]]; then
        rm -rf "$DST_DIR"
        mv "$SRC_ARCHIVE" "$DST_DIR"
        ok "Ripetizione ${i} archiviata → $DST_DIR"
    else
        warn "Archivio atteso non trovato: $SRC_ARCHIVE — salto l'archiviazione per la ripetizione ${i}."
    fi
done

sep
log "Aggregazione delle ${N_REPEATS} ripetizioni..."

python3 - "$EXPERIMENT_DIR" "$QUERY" "$N_REPEATS" << 'PYEOF'
import re
import statistics as stats
import sys

exp_dir, query, n = sys.argv[1], sys.argv[2], int(sys.argv[3])


def extract_q1(text):
    patterns = {
        "throughput_sistema_rps": r"Throughput sistema \(rps\)\s*:\s*([\d.]+)",
        "latenza_mean_ms":        r"Latenza ms\s+mean\s*:\s*([\d.]+)",
        "latenza_median_ms":      r"Latenza ms\s+median\s*:\s*([\d.]+)",
        "filter_rps_mean":        r"filter_rps mean\s*:\s*([\d.]+)",
        "busy_pct_mean":          r"Busy ms/s\s+mean\s*:\s*[\d.]+\s*\(([\d.]+)%",
        "scarto_relativo_pct":    r"Scarto relativo\s*:\s*([\d.]+)%",
    }
    out = {}
    for key, pat in patterns.items():
        m = re.search(pat, text)
        if m:
            out[key] = float(m.group(1))
    return out


def extract_q2(text):
    # Formato write_summary_q2 in compare_metrics.py: tre blocchi [1h]/[6h]/
    # [global] sia nella sezione esterna (latenza, throughput rps) sia in
    # quella interna per-ramo (busy%) — vanno estratti separatamente, un
    # singolo pattern "prima occorrenza" prenderebbe solo il ramo 1h e
    # ometterebbe 6h/global, oltre a non trovare affatto le chiavi che qui
    # semplicemente non esistono in forma "unica" (es. Busy % globale).
    out = {}

    ext_match = re.search(r"CANALE ESTERNO(.*?)CANALE INTERNO — SOURCE", text, re.DOTALL)
    ext_section = ext_match.group(1) if ext_match else ""
    for branch in ("1h", "6h", "global"):
        block_match = re.search(
            rf"\[{re.escape(branch)}\]\s+\d+ trigger(.*?)(?=\n  \[|\Z)",
            ext_section, re.DOTALL,
        )
        if block_match:
            block = block_match.group(1)
            m = re.search(r"Latenza ms\s+mean\s*:\s*([\d.]+)", block)
            if m:
                out[f"latenza_mean_ms_{branch}"] = float(m.group(1))
            m = re.search(r"Latenza ms\s+median\s*:\s*([\d.]+)", block)
            if m:
                out[f"latenza_median_ms_{branch}"] = float(m.group(1))
            m = re.search(r"Throughput rps mean\s*:\s*([\d.]+)", block)
            if m:
                out[f"throughput_rps_mean_{branch}"] = float(m.group(1))

    m = re.search(r"filter_rps mean\s*:\s*([\d.]+)", text)
    if m:
        out["filter_rps_mean"] = float(m.group(1))

    branch_match = re.search(r"PER RAMO WINDOW(.*?)CONFRONTO NELLA", text, re.DOTALL)
    branch_section = branch_match.group(1) if branch_match else ""
    for branch in ("1h", "6h", "global"):
        block_match = re.search(
            rf"\[{re.escape(branch)}\]\n(.*?)(?=\n  \[|\Z)",
            branch_section, re.DOTALL,
        )
        if block_match:
            block = block_match.group(1)
            m = re.search(r"Busy ms/s\s+mean\s*:\s*[\d.]+\s*\(([\d.]+)%", block)
            if m:
                out[f"busy_pct_mean_{branch}"] = float(m.group(1))

    m = re.search(r"Canale esterno \(derivato\):\s*([\d.]+)\s*rec/s", text)
    if m:
        out["throughput_sistema_rps"] = float(m.group(1))
    m = re.search(r"Scarto relativo\s*:\s*([\d.]+)%", text)
    if m:
        out["scarto_relativo_pct"] = float(m.group(1))

    return out


EXTRACTORS = {"q1": extract_q1, "q2": extract_q2}
extractor = EXTRACTORS[query]

# L'ordine e le chiavi non sono note a priori per Q2 (dipendono da quali
# branch sono presenti nel file), quindi si raccolgono dinamicamente invece
# di usare un dict di chiavi fisse come per Q1.
results = {}
missing = []

for i in range(1, n + 1):
    path = f"{exp_dir}/run{i}/metrics_summary_{query}.txt"
    try:
        with open(path, encoding="utf-8") as f:
            text = f.read()
    except FileNotFoundError:
        missing.append(path)
        continue
    for key, val in extractor(text).items():
        results.setdefault(key, []).append(val)

lines = []
lines.append(f"AGGREGAZIONE SU {n} RIPETIZIONI — query={query}")
lines.append("=" * 70)
if missing:
    lines.append("")
    lines.append("ATTENZIONE — file non trovati (ripetizioni escluse dall'aggregazione):")
    for p in missing:
        lines.append(f"  - {p}")
lines.append("")

LABELS = {
    "throughput_sistema_rps": "Throughput sistema (rps)",
    "latenza_mean_ms":        "Latenza mean (ms)",
    "latenza_median_ms":      "Latenza median (ms)",
    "filter_rps_mean":        "filter_rps mean (rec/s)",
    "busy_pct_mean":          "Busy % mean",
    "scarto_relativo_pct":    "Scarto relativo canali (%)",
    "latenza_mean_ms_1h":       "Latenza mean (ms) [1h]",
    "latenza_mean_ms_6h":       "Latenza mean (ms) [6h]",
    "latenza_mean_ms_global":   "Latenza mean (ms) [global]",
    "latenza_median_ms_1h":     "Latenza median (ms) [1h]",
    "latenza_median_ms_6h":     "Latenza median (ms) [6h]",
    "latenza_median_ms_global": "Latenza median (ms) [global]",
    "throughput_rps_mean_1h":     "Throughput rps mean [1h]",
    "throughput_rps_mean_6h":     "Throughput rps mean [6h]",
    "throughput_rps_mean_global": "Throughput rps mean [global]",
    "busy_pct_mean_1h":       "Busy % mean [1h]",
    "busy_pct_mean_6h":       "Busy % mean [6h]",
    "busy_pct_mean_global":   "Busy % mean [global]",
}

# Ordine di stampa fisso e leggibile invece dell'ordine di inserimento nel dict.
ORDER = [
    "throughput_sistema_rps",
    "filter_rps_mean",
    "latenza_mean_ms", "latenza_median_ms",
    "latenza_mean_ms_1h", "latenza_mean_ms_6h", "latenza_mean_ms_global",
    "latenza_median_ms_1h", "latenza_median_ms_6h", "latenza_median_ms_global",
    "throughput_rps_mean_1h", "throughput_rps_mean_6h", "throughput_rps_mean_global",
    "busy_pct_mean",
    "busy_pct_mean_1h", "busy_pct_mean_6h", "busy_pct_mean_global",
    "scarto_relativo_pct",
]
keys_to_print = [k for k in ORDER if k in results] + \
                 [k for k in results if k not in ORDER]

for key in keys_to_print:
    vals = results[key]
    label = LABELS.get(key, key)
    if not vals:
        lines.append(f"{label:32s}: nessun dato estratto")
        continue
    mean = stats.mean(vals)
    sd = stats.stdev(vals) if len(vals) > 1 else 0.0
    cv = (sd / mean * 100) if mean else 0.0
    vals_str = ", ".join(f"{v:.4f}" for v in vals)
    lines.append(
        f"{label:32s}: mean={mean:10.4f}  stdev={sd:8.4f}  cv={cv:5.1f}%   valori=[{vals_str}]"
    )

lines.append("")
lines.append("cv (coefficiente di variazione) = stdev/mean*100 — indica quanto")
lines.append("rumore c'e' a parita' di configurazione. Una differenza tra parallelism")
lines.append("diversi e' un segnale credibile solo se supera chiaramente questo cv%.")

out = "\n".join(lines)
print(out)
with open(f"{exp_dir}/aggregate_summary.txt", "w", encoding="utf-8") as f:
    f.write(out + "\n")
PYEOF

sep
ok "Esperimento completato. Risultati in: $EXPERIMENT_DIR"
echo "  Aggregazione: $EXPERIMENT_DIR/aggregate_summary.txt"
sep