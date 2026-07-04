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

# Scopre TUTTE le chiavi metrica che contengono il pattern, non solo la prima.
# Necessario perché con parallelism > partizioni Kafka disponibili esiste
# una chiave per subtask (es. "0.Filter...", "1.Filter..."), e solo una è
# quella del subtask che riceve davvero dati — prendere "la prima" (come
# faceva discover_metric_names) rischia di selezionare un subtask idle e
# campionare zero per tutta la sessione, anche se la pipeline lavora
# correttamente altrove.
discover_all_metric_keys() {
    local job_id="$1"
    local vertex_id="$2"
    local pattern="$3"

    if [ -z "$vertex_id" ]; then
        return
    fi

    curl -sf "$FLINK_URL/jobs/$job_id/vertices/$vertex_id/metrics" \
        | python3 -c "
import sys, json
pattern = sys.argv[1]
try:
    metrics = json.load(sys.stdin)
    names = [m['id'] for m in metrics]
except Exception:
    names = []
for n in names:
    if pattern in n:
        print(n)
" "$pattern" 2>/dev/null
}

# Somma i valori di più chiavi in un'unica chiamata REST (una fetch_batch +
# somma in Python). Usato per throughput (numRecordsOutPerSecond): esattamente
# un subtask è attivo, gli altri restano a zero — la somma dà il valore
# corretto indipendentemente da quale subtask sia quello attivo.
fetch_sum() {
    local job_id="$1"
    local vertex_id="$2"
    shift 2
    local keys=("$@")

    if [ -z "$vertex_id" ] || [ ${#keys[@]} -eq 0 ]; then
        echo "0.0"
        return
    fi

    local vals
    vals=$(fetch_batch "$job_id" "$vertex_id" "${keys[@]}")
    python3 -c "
import sys
vals = sys.argv[1].split(',')
print(f'{sum(float(v) for v in vals if v):.4f}')
" "$vals"
}

# Massimo tra più chiavi. Usato per busy/backpressure/idle time: ha senso
# il picco tra i subtask (il più carico), non la somma — stessa convenzione
# "Busy (max)" già usata dalla Flink Web UI nel grafo del job.
fetch_max() {
    local job_id="$1"
    local vertex_id="$2"
    shift 2
    local keys=("$@")

    if [ -z "$vertex_id" ] || [ ${#keys[@]} -eq 0 ]; then
        echo "0.0"
        return
    fi

    local vals
    vals=$(fetch_batch "$job_id" "$vertex_id" "${keys[@]}")
    python3 -c "
import sys
vals = sys.argv[1].split(',')
nums = [float(v) for v in vals if v]
print(f'{max(nums) if nums else 0.0:.4f}')
" "$vals"
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

echo "[poll] Scoperta nomi metrica sul vertex SOURCE (tutti i subtask)..."
mapfile -t SRC_KAFKA_KEYS < <(discover_all_metric_keys "$JOB_ID" "$VID_SOURCE" "Source__Kafka_flights_source.numRecordsOutPerSecond")
mapfile -t SRC_FILTER_KEYS < <(discover_all_metric_keys "$JOB_ID" "$VID_SOURCE" "Filter.numRecordsOutPerSecond")
mapfile -t SRC_BUSY_KEYS < <(discover_all_metric_keys "$JOB_ID" "$VID_SOURCE" "busyTimeMsPerSecond")
mapfile -t SRC_BACKPRESSURE_KEYS < <(discover_all_metric_keys "$JOB_ID" "$VID_SOURCE" "backPressuredTimeMsPerSecond")
mapfile -t SRC_IDLE_KEYS < <(discover_all_metric_keys "$JOB_ID" "$VID_SOURCE" "idleTimeMsPerSecond")

[ ${#SRC_KAFKA_KEYS[@]} -eq 0 ]        && SRC_KAFKA_KEYS=("0.numRecordsOutPerSecond")
[ ${#SRC_FILTER_KEYS[@]} -eq 0 ]       && SRC_FILTER_KEYS=("0.numRecordsOutPerSecond")
[ ${#SRC_BUSY_KEYS[@]} -eq 0 ]         && SRC_BUSY_KEYS=("0.busyTimeMsPerSecond")
[ ${#SRC_BACKPRESSURE_KEYS[@]} -eq 0 ] && SRC_BACKPRESSURE_KEYS=("0.backPressuredTimeMsPerSecond")
[ ${#SRC_IDLE_KEYS[@]} -eq 0 ]         && SRC_IDLE_KEYS=("0.idleTimeMsPerSecond")

echo "[poll]   Kafka source key(s) : ${SRC_KAFKA_KEYS[*]}  (sommate)"
echo "[poll]   Filter key(s)       : ${SRC_FILTER_KEYS[*]}  (sommate)"
echo "[poll]   Busy key(s)         : ${SRC_BUSY_KEYS[*]}  (massimo)"
echo ""

WIN_KEYS=(
    "0.numRecordsInPerSecond"
    "0.numRecordsOutPerSecond"
    "0.busyTimeMsPerSecond"
    "0.backPressuredTimeMsPerSecond"
    "0.idleTimeMsPerSecond"
)

# Solo per Q1: il vertex WINDOW è dopo un keyBy(airline) reale, quindi eredita
# il parallelism del job (fino a 4, una per compagnia) — stesso motivo per cui
# serviva la somma su tutti i subtask per il vertex SOURCE. Le tre finestre di
# Q2 (windowAll) restano invece SEMPRE a parallelism 1 per costruzione, quindi
# per Q2 "0." è già corretto e WIN_KEYS sopra resta invariato.
if [ "$QUERY" != "q2" ]; then
    mapfile -t WIN_IN_KEYS  < <(discover_all_metric_keys "$JOB_ID" "$VID_WINDOW" "numRecordsInPerSecond")
    mapfile -t WIN_OUT_KEYS < <(discover_all_metric_keys "$JOB_ID" "$VID_WINDOW" "numRecordsOutPerSecond")
    mapfile -t WIN_BUSY_KEYS < <(discover_all_metric_keys "$JOB_ID" "$VID_WINDOW" "busyTimeMsPerSecond")
    mapfile -t WIN_BACKPRESSURE_KEYS < <(discover_all_metric_keys "$JOB_ID" "$VID_WINDOW" "backPressuredTimeMsPerSecond")
    mapfile -t WIN_IDLE_KEYS < <(discover_all_metric_keys "$JOB_ID" "$VID_WINDOW" "idleTimeMsPerSecond")

    [ ${#WIN_IN_KEYS[@]} -eq 0 ]            && WIN_IN_KEYS=("0.numRecordsInPerSecond")
    [ ${#WIN_OUT_KEYS[@]} -eq 0 ]           && WIN_OUT_KEYS=("0.numRecordsOutPerSecond")
    [ ${#WIN_BUSY_KEYS[@]} -eq 0 ]          && WIN_BUSY_KEYS=("0.busyTimeMsPerSecond")
    [ ${#WIN_BACKPRESSURE_KEYS[@]} -eq 0 ]  && WIN_BACKPRESSURE_KEYS=("0.backPressuredTimeMsPerSecond")
    [ ${#WIN_IDLE_KEYS[@]} -eq 0 ]          && WIN_IDLE_KEYS=("0.idleTimeMsPerSecond")

    echo "[poll]   Window in/out key(s) : ${WIN_IN_KEYS[*]} | ${WIN_OUT_KEYS[*]}  (sommate)"
fi

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

    SRC_KAFKA_RPS=$(fetch_sum "$JOB_ID" "$VID_SOURCE" "${SRC_KAFKA_KEYS[@]}")
    SRC_FILTER_RPS=$(fetch_sum "$JOB_ID" "$VID_SOURCE" "${SRC_FILTER_KEYS[@]}")
    SRC_BUSY_MS=$(fetch_max "$JOB_ID" "$VID_SOURCE" "${SRC_BUSY_KEYS[@]}")
    SRC_BACKPRESSURE_MS=$(fetch_max "$JOB_ID" "$VID_SOURCE" "${SRC_BACKPRESSURE_KEYS[@]}")
    SRC_IDLE_MS=$(fetch_max "$JOB_ID" "$VID_SOURCE" "${SRC_IDLE_KEYS[@]}")
    SOURCE_VALS="${SRC_KAFKA_RPS},${SRC_FILTER_RPS},${SRC_BUSY_MS},${SRC_BACKPRESSURE_MS},${SRC_IDLE_MS}"

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
        WIN_RECORDS_IN=$(fetch_sum "$JOB_ID" "$VID_WINDOW" "${WIN_IN_KEYS[@]}")
        WIN_RECORDS_OUT=$(fetch_sum "$JOB_ID" "$VID_WINDOW" "${WIN_OUT_KEYS[@]}")
        WIN_BUSY_MS=$(fetch_max "$JOB_ID" "$VID_WINDOW" "${WIN_BUSY_KEYS[@]}")
        WIN_BACKPRESSURE_MS=$(fetch_max "$JOB_ID" "$VID_WINDOW" "${WIN_BACKPRESSURE_KEYS[@]}")
        WIN_IDLE_MS=$(fetch_max "$JOB_ID" "$VID_WINDOW" "${WIN_IDLE_KEYS[@]}")
        # sink_csv_records_in_per_sec: nessun vertex Sink dedicato viene scoperto
        # da questo script — il valore precedente qui interrogava per errore
        # (copia-incolla) la stessa metrica del vertex WINDOW una seconda volta.
        # Placeholder onesto a 0.0 finché non si implementa una vera discovery
        # del vertex Sink; la colonna resta nello schema per compatibilità con
        # compare_metrics.py (che la legge ma non la usa nelle statistiche).
        WINDOW_VALS="${WIN_RECORDS_IN},${WIN_RECORDS_OUT},0.0,${WIN_BUSY_MS},${WIN_BACKPRESSURE_MS},${WIN_IDLE_MS}"
        echo "${TS_EPOCH},${TS_ISO},${JOB_ID},${SOURCE_VALS},${WINDOW_VALS}" >> "$OUTPUT_FILE"

        if (( SAMPLE % 10 == 0 )) || (( SAMPLE == 1 )); then
            FILTER_RPS=$(echo "$SOURCE_VALS" | cut -d',' -f2)
            WIN_OUT="$WIN_RECORDS_OUT"
            BUSY_MS="$WIN_BUSY_MS"
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