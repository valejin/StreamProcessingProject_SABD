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
# FUNZIONAMENTO — AUTO-DISCOVERY MULTI-VERTEX:
#   A differenza della versione precedente (che assumeva sempre 2
#   vertex: source+filter e window+sink), questo poller scopre TUTTI
#   i vertex del job e li classifica per ruolo:
#
#     SOURCE  : vertex con "Kafka"/"Source" nel nome
#               (Q1: un solo vertex Source+Filter+keyBy fuso)
#               (Q2: un solo vertex Source+filtro heartbeat/completed)
#
#     WINDOW* : vertex con "TumblingEventTimeWindows",
#               "SlidingEventTimeWindows", o "GlobalWindows" nel nome
#               (Q1: un solo vertex window)
#               (Q2: TRE vertex separati: 1h, 6h, global — ciascuno
#                    campionato su colonne CSV distinte)
#
#   Questo risolve il mismatch della versione precedente, dove con
#   3 windowAll paralleli il loop di discovery sovrascriveva
#   VID_WINDOW tenendo solo l'ultimo vertex trovato (tipicamente
#   "global"), facendo leggere filter_rps dal vertex sbagliato.
#
# OUTPUT CSV (schema esteso rispetto alla versione precedente):
#   Q1: stesse colonne di prima (compatibilità totale)
#   Q2: colonne source_* identiche, più win1h_*, win6h_*,
#       winglobal_* per ciascuna delle tre finestre separate
# ============================================================

set -euo pipefail

QUERY="${1:-q1}"
INTERVAL="${2:-5}"
FLINK_URL="${FLINK_URL:-http://localhost:8081}"
OUTPUT_DIR="${OUTPUT_DIR:-./Results}"
OUTPUT_FILE="$OUTPUT_DIR/metrics_flink_internal_${QUERY}.csv"

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

discover_vertices() {
    local job_id="$1"
    curl -sf "$FLINK_URL/jobs/$job_id" \
        | python3 -c "
import sys, json

data = json.load(sys.stdin)
vertices = data.get('vertices', [])

vid_source        = ''
vid_window_1h     = ''
vid_window_6h     = ''
vid_window_global = ''
vid_window_generic = ''

window_keywords = ['TumblingEventTimeWindows', 'SlidingEventTimeWindows', 'GlobalWindows']

for v in vertices:
    name = v['name']
    vid  = v['id']

    is_source_like = any(k in name for k in ['Kafka', 'Source'])
    is_window_like = any(k in name for k in window_keywords)

    if is_source_like and not is_window_like:
        vid_source = vid
        continue

    if is_window_like:
        if 'GlobalWindows' in name:
            vid_window_global = vid
        elif 'SlidingEventTimeWindows' in name:
            if not vid_window_1h:
                vid_window_1h = vid
            elif not vid_window_6h:
                vid_window_6h = vid
        elif 'TumblingEventTimeWindows' in name:
            vid_window_1h = vid
        if not vid_window_generic:
            vid_window_generic = vid

if not vid_source and vertices:
    vid_source = vertices[0]['id']

print(f'VID_SOURCE={vid_source}')
print(f'VID_WINDOW_1H={vid_window_1h}')
print(f'VID_WINDOW_6H={vid_window_6h}')
print(f'VID_WINDOW_GLOBAL={vid_window_global}')
print(f'VID_WINDOW={vid_window_generic}')

for v in vertices:
    print(f'[poll]   vertex: {v[\"id\"][:8]}... -> {v[\"name\"][:70]}', file=sys.stderr)
" 2>/dev/null
}

fetch_batch() {
    local job_id="$1"
    local vertex_id="$2"
    shift 2
    local keys=("$@")

    if [ -z "$vertex_id" ]; then
        python3 -c "print(','.join('0.0' for _ in range(${#keys[@]})))"
        return
    fi

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

discover_metric_names() {
    local job_id="$1"
    local vertex_id="$2"
    shift 2
    local patterns=("$@")

    if [ -z "$vertex_id" ]; then
        for _ in "${patterns[@]}"; do echo ""; done
        return
    fi

    curl -sf "$FLINK_URL/jobs/$job_id/vertices/$vertex_id/metrics" \
        | python3 -c "
import sys, json
patterns = sys.argv[1:]
try:
    metrics = json.load(sys.stdin)
    names = [m['id'] for m in metrics]
except Exception:
    names = []
for p in patterns:
    match = next((n for n in names if p in n), '')
    print(match)
" "${patterns[@]}" 2>/dev/null
}

echo "[poll] Ricerca job RUNNING su $FLINK_URL ..."
JOB_ID=$(get_running_job_id)

if [ -z "$JOB_ID" ]; then
    echo "[poll] ERRORE: nessun job RUNNING trovato."
    exit 1
fi
echo "[poll] Job ID: $JOB_ID"

echo "[poll] Scoperta vertex del job..."
eval "$(discover_vertices "$JOB_ID")"

if [ -z "$VID_SOURCE" ]; then
    echo "[poll] ERRORE: vertex SOURCE non trovato."
    exit 1
fi
echo "[poll] Vertex SOURCE        : ${VID_SOURCE}"
echo "[poll] Vertex WINDOW 1h     : ${VID_WINDOW_1H:-<non trovato>}"
echo "[poll] Vertex WINDOW 6h     : ${VID_WINDOW_6H:-<non trovato, normale per Q1>}"
echo "[poll] Vertex WINDOW global : ${VID_WINDOW_GLOBAL:-<non trovato, normale per Q1>}"
echo "[poll] Output               : $OUTPUT_FILE"
echo "[poll] Intervallo           : ${INTERVAL}s"
echo ""

echo "[poll] Scoperta nomi metrica sul vertex SOURCE..."
SRC_METRIC_NAMES=$(discover_metric_names "$JOB_ID" "$VID_SOURCE" \
    "Source__Kafka_flights_source.numRecordsOutPerSecond" \
    "Filter.numRecordsOutPerSecond")
SRC_KAFKA_KEY=$(echo "$SRC_METRIC_NAMES" | sed -n '1p')
SRC_FILTER_KEY=$(echo "$SRC_METRIC_NAMES" | sed -n '2p')

[ -z "$SRC_KAFKA_KEY" ]  && SRC_KAFKA_KEY="0.numRecordsOutPerSecond"
[ -z "$SRC_FILTER_KEY" ] && SRC_FILTER_KEY="0.numRecordsOutPerSecond"

echo "[poll]   Kafka source key : ${SRC_KAFKA_KEY}"
echo "[poll]   Filter key       : ${SRC_FILTER_KEY}"
echo ""

SRC_KEYS=(
    "$SRC_KAFKA_KEY"
    "$SRC_FILTER_KEY"
    "0.busyTimeMsPerSecond"
    "0.backPressuredTimeMsPerSecond"
    "0.idleTimeMsPerSecond"
)

WIN_KEYS=(
    "0.numRecordsInPerSecond"
    "0.numRecordsOutPerSecond"
    "0.busyTimeMsPerSecond"
    "0.backPressuredTimeMsPerSecond"
    "0.idleTimeMsPerSecond"
)

mkdir -p "$OUTPUT_DIR"

if [ "$QUERY" = "q2" ]; then
    HEADER="timestamp_epoch,timestamp_iso,job_id,"
    HEADER+="source_records_out_per_sec,filter_records_out_per_sec,"
    HEADER+="source_busy_ms_per_sec,source_backpressure_ms_per_sec,source_idle_ms_per_sec,"
    HEADER+="win1h_records_in_per_sec,win1h_records_out_per_sec,win1h_busy_ms_per_sec,win1h_backpressure_ms_per_sec,win1h_idle_ms_per_sec,"
    HEADER+="win6h_records_in_per_sec,win6h_records_out_per_sec,win6h_busy_ms_per_sec,win6h_backpressure_ms_per_sec,win6h_idle_ms_per_sec,"
    HEADER+="winglobal_records_in_per_sec,winglobal_records_out_per_sec,winglobal_busy_ms_per_sec,winglobal_backpressure_ms_per_sec,winglobal_idle_ms_per_sec"
else
    HEADER="timestamp_epoch,timestamp_iso,job_id,"
    HEADER+="source_records_out_per_sec,filter_records_out_per_sec,"
    HEADER+="source_busy_ms_per_sec,source_backpressure_ms_per_sec,source_idle_ms_per_sec,"
    HEADER+="window_records_in_per_sec,window_records_out_per_sec,sink_csv_records_in_per_sec,"
    HEADER+="window_busy_ms_per_sec,window_backpressure_ms_per_sec,window_idle_ms_per_sec"
fi
echo "$HEADER" > "$OUTPUT_FILE"

SAMPLE=0
while is_job_running "$JOB_ID"; do
    SAMPLE=$((SAMPLE + 1))
    TS_EPOCH=$(date +%s)
    TS_ISO=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

    SOURCE_VALS=$(fetch_batch "$JOB_ID" "$VID_SOURCE" "${SRC_KEYS[@]}")

    if [ "$QUERY" = "q2" ]; then
        WIN1H_VALS=$(fetch_batch "$JOB_ID" "$VID_WINDOW_1H"     "${WIN_KEYS[@]}")
        WIN6H_VALS=$(fetch_batch "$JOB_ID" "$VID_WINDOW_6H"     "${WIN_KEYS[@]}")
        WINGL_VALS=$(fetch_batch "$JOB_ID" "$VID_WINDOW_GLOBAL" "${WIN_KEYS[@]}")
        echo "${TS_EPOCH},${TS_ISO},${JOB_ID},${SOURCE_VALS},${WIN1H_VALS},${WIN6H_VALS},${WINGL_VALS}" >> "$OUTPUT_FILE"

        if (( SAMPLE % 10 == 0 )) || (( SAMPLE == 1 )); then
            FILTER_RPS=$(echo "$SOURCE_VALS" | cut -d',' -f2)
            W1H_OUT=$(echo "$WIN1H_VALS" | cut -d',' -f2)
            W6H_OUT=$(echo "$WIN6H_VALS" | cut -d',' -f2)
            WGL_OUT=$(echo "$WINGL_VALS" | cut -d',' -f2)
            WGL_BUSY=$(echo "$WINGL_VALS" | cut -d',' -f4)
            printf "[%s] sample=%-4d | filter=%8.2f rec/s | 1h_out=%6.3f 6h_out=%6.3f global_out=%6.3f | global_busy=%4.0f ms/s\n" \
                "$TS_ISO" "$SAMPLE" "$FILTER_RPS" "$W1H_OUT" "$W6H_OUT" "$WGL_OUT" "$WGL_BUSY"
        fi
    else
        WINDOW_VALS=$(fetch_batch "$JOB_ID" "$VID_WINDOW" \
            "0.numRecordsInPerSecond" "0.numRecordsOutPerSecond" \
            "0.numRecordsInPerSecond" \
            "0.busyTimeMsPerSecond" "0.backPressuredTimeMsPerSecond" "0.idleTimeMsPerSecond")
        echo "${TS_EPOCH},${TS_ISO},${JOB_ID},${SOURCE_VALS},${WINDOW_VALS}" >> "$OUTPUT_FILE"

        if (( SAMPLE % 10 == 0 )) || (( SAMPLE == 1 )); then
            FILTER_RPS=$(echo "$SOURCE_VALS" | cut -d',' -f2)
            WIN_OUT=$(echo    "$WINDOW_VALS" | cut -d',' -f2)
            BUSY_MS=$(echo    "$WINDOW_VALS" | cut -d',' -f4)
            printf "[%s] sample=%-4d | filter=%8.2f rec/s | window_out=%8.4f rec/s | busy=%4.0f ms/s\n" \
                "$TS_ISO" "$SAMPLE" "$FILTER_RPS" "$WIN_OUT" "$BUSY_MS"
        fi
    fi

    sleep "$INTERVAL"
done

echo ""
echo "[poll] Job $JOB_ID terminato. Polling completato."
echo "[poll] Campioni totali : $SAMPLE"
echo "[poll] CSV salvato in  : $OUTPUT_FILE"