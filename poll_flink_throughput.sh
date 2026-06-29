#!/usr/bin/env bash
# ============================================================
# poll_flink_throughput.sh
#
# SCOPO: campiona periodicamente le metriche interne di Flink
#        via REST API e le salva in metrics_flink_internal_<query>.csv
#
# COME USARE:
#   bash poll_flink_throughput.sh [q1|q2] [INTERVALLO_SEC]
#   (normalmente chiamato da run_pipeline.sh, non direttamente)
#
# FUNZIONAMENTO:
#   - Auto-rileva JOB_ID del job RUNNING
#   - Auto-scopre i vertex ID dal job (nessun ID hardcoded)
#   - Identifica i vertex per pattern nel nome:
#       SOURCE: contiene "Kafka" o "Filter"
#       WINDOW: contiene "TumblingEventTimeWindows" o "SlidingEventTimeWindows"
#   - Campiona ogni INTERVALLO_SEC secondi
#   - Si ferma automaticamente quando il job non è più RUNNING
# ============================================================

set -euo pipefail

# ── Configurazione ────────────────────────────────────────────
QUERY="${1:-q1}"
INTERVAL="${2:-5}"
FLINK_URL="${FLINK_URL:-http://localhost:8081}"
OUTPUT_DIR="${OUTPUT_DIR:-./Results}"
OUTPUT_FILE="$OUTPUT_DIR/metrics_flink_internal_${QUERY}.csv"

# ── Funzioni base ─────────────────────────────────────────────

get_running_job_id() {
    curl -sf "$FLINK_URL/jobs" \
        | python3 -c "
import sys, json
jobs = json.load(sys.stdin).get('jobs', [])
running = [j for j in jobs if j.get('status') == 'RUNNING']
print(running[0]['id'] if running else '')
" 2>/dev/null || echo ""
}

is_job_running() {
    local job_id="$1"
    local status
    status=$(curl -sf "$FLINK_URL/jobs/$job_id" \
        | python3 -c "
import sys, json
print(json.load(sys.stdin).get('state','UNKNOWN'))
" 2>/dev/null) || status="UNKNOWN"
    [ "$status" = "RUNNING" ]
}

# Scopre i vertex ID dal job tramite pattern matching sul nome
# Stampa: VID_SOURCE e VID_WINDOW come variabili bash
discover_vertices() {
    local job_id="$1"
    curl -sf "$FLINK_URL/jobs/$job_id" \
        | python3 -c "
import sys, json

data = json.load(sys.stdin)
vertices = data.get('vertices', [])

vid_source = ''
vid_window = ''

for v in vertices:
    name = v['name']
    vid  = v['id']
    # Vertex SOURCE: contiene la sorgente Kafka + Filter (sempre il primo chain)
    if any(k in name for k in ['Kafka', 'Source', 'Filter']):
        # Prendiamo quello con 'Filter' nel nome (chain Source+Filter)
        if 'Filter' in name or 'Source' in name:
            vid_source = vid
    # Vertex WINDOW: contiene la window function
    if any(k in name for k in ['TumblingEventTimeWindows',
                                 'SlidingEventTimeWindows',
                                 'GlobalWindows']):
        vid_window = vid

# Fallback: se non trovati per nome, usa ordine (Source=0, Window=1)
if not vid_source and vertices:
    vid_source = vertices[0]['id']
if not vid_window and len(vertices) > 1:
    vid_window = vertices[1]['id']

print(f'VID_SOURCE={vid_source}')
print(f'VID_WINDOW={vid_window}')

# Log dei vertex trovati su stderr
for v in vertices:
    print(f'[poll]   vertex: {v[\"id\"][:8]}... → {v[\"name\"][:60]}', file=sys.stderr)
" 2>/dev/null
}

# Legge una lista di metriche da un vertex in una sola chiamata HTTP
# Argomenti: job_id vertex_id metrica1 metrica2 ...
# Output:    val1,val2,... (stesso ordine)
fetch_batch() {
    local job_id="$1"
    local vertex_id="$2"
    shift 2
    local keys=("$@")

    # Costruisce ?get=key1,key2,... con URL encoding
    local get_param
    get_param=$(python3 -c "
import urllib.parse, sys
print(urllib.parse.quote(','.join(sys.argv[1:])))
" "${keys[@]}")

    curl -sf "$FLINK_URL/jobs/$job_id/vertices/$vertex_id/metrics?get=$get_param" \
        | python3 -c "
import sys, json
keys = sys.argv[1:]
try:
    data = json.load(sys.stdin)
    vals = {item['id']: item['value'] for item in data}
    print(','.join(vals.get(k, '0.0') for k in keys))
except Exception:
    print(','.join('0.0' for _ in keys))
" "${keys[@]}" 2>/dev/null \
    || python3 -c "import sys; print(','.join('0.0' for _ in sys.argv[1:]))" "${keys[@]}"
}

# ── Ricerca job RUNNING ───────────────────────────────────────
echo "[poll] Ricerca job RUNNING su $FLINK_URL ..."
JOB_ID=$(get_running_job_id)

if [ -z "$JOB_ID" ]; then
    echo "[poll] ERRORE: nessun job RUNNING trovato."
    exit 1
fi
echo "[poll] Job ID: $JOB_ID"

# ── Auto-discovery vertex ─────────────────────────────────────
echo "[poll] Scoperta vertex del job..."
eval "$(discover_vertices "$JOB_ID")"

if [ -z "$VID_SOURCE" ] || [ -z "$VID_WINDOW" ]; then
    echo "[poll] ERRORE: vertex SOURCE o WINDOW non trovati."
    exit 1
fi
echo "[poll] Vertex SOURCE : ${VID_SOURCE}"
echo "[poll] Vertex WINDOW : ${VID_WINDOW}"
echo "[poll] Output        : $OUTPUT_FILE"
echo "[poll] Intervallo    : ${INTERVAL}s"
echo ""

# ── Nomi metriche (costanti, indipendenti dai vertex ID) ──────
# Vertex SOURCE+FILTER
SRC_KEYS=(
    "0.Source__Kafka_flights_source.numRecordsOutPerSecond"
    "0.Filter.numRecordsOutPerSecond"
    "0.busyTimeMsPerSecond"
    "0.backPressuredTimeMsPerSecond"
    "0.idleTimeMsPerSecond"
)

# Vertex WINDOW+SINKS — il nome della window cambia tra Q1 e Q2
if [ "$QUERY" = "q1" ]; then
    WIN_WINDOW_KEY="0.TumblingEventTimeWindows.numRecordsOutPerSecond"
    WIN_WINDOW_IN_KEY="0.TumblingEventTimeWindows.numRecordsInPerSecond"
    WIN_SINK_KEY="0.Sink__CSV_Sink_Q1.numRecordsInPerSecond"
else
    # Q2 ha finestre sliding e global: usiamo la metrica aggregata del vertex
    WIN_WINDOW_KEY="0.numRecordsOutPerSecond"
    WIN_WINDOW_IN_KEY="0.numRecordsInPerSecond"
    WIN_SINK_KEY="0.Sink__CSV_Sink_Q2_1h.numRecordsInPerSecond"
fi

WIN_KEYS=(
    "$WIN_WINDOW_IN_KEY"
    "$WIN_WINDOW_KEY"
    "$WIN_SINK_KEY"
    "0.busyTimeMsPerSecond"
    "0.backPressuredTimeMsPerSecond"
    "0.idleTimeMsPerSecond"
)

# ── Intestazione CSV ──────────────────────────────────────────
mkdir -p "$OUTPUT_DIR"
{
echo "timestamp_epoch,timestamp_iso,job_id,\
source_records_out_per_sec,filter_records_out_per_sec,\
source_busy_ms_per_sec,source_backpressure_ms_per_sec,source_idle_ms_per_sec,\
window_records_in_per_sec,window_records_out_per_sec,sink_csv_records_in_per_sec,\
window_busy_ms_per_sec,window_backpressure_ms_per_sec,window_idle_ms_per_sec"
} > "$OUTPUT_FILE"

# ── Loop di campionamento ─────────────────────────────────────
SAMPLE=0
while is_job_running "$JOB_ID"; do
    SAMPLE=$((SAMPLE + 1))
    TS_EPOCH=$(date +%s)
    TS_ISO=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

    SOURCE_VALS=$(fetch_batch "$JOB_ID" "$VID_SOURCE" "${SRC_KEYS[@]}")
    WINDOW_VALS=$(fetch_batch "$JOB_ID" "$VID_WINDOW" "${WIN_KEYS[@]}")

    echo "${TS_EPOCH},${TS_ISO},${JOB_ID},${SOURCE_VALS},${WINDOW_VALS}" >> "$OUTPUT_FILE"

    # Log a terminale ogni 10 campioni (o al primo)
    if (( SAMPLE % 10 == 0 )) || (( SAMPLE == 1 )); then
        FILTER_RPS=$(echo "$SOURCE_VALS" | cut -d',' -f2)
        WIN_OUT=$(echo    "$WINDOW_VALS" | cut -d',' -f2)
        BUSY_MS=$(echo    "$WINDOW_VALS" | cut -d',' -f4)
        printf "[%s] sample=%-4d | filter=%8.2f rec/s | window_out=%8.4f rec/s | busy=%4.0f ms/s\n" \
            "$TS_ISO" "$SAMPLE" "$FILTER_RPS" "$WIN_OUT" "$BUSY_MS"
    fi

    sleep "$INTERVAL"
done

echo ""
echo "[poll] Job $JOB_ID terminato. Polling completato."
echo "[poll] Campioni totali : $SAMPLE"
echo "[poll] CSV salvato in  : $OUTPUT_FILE"