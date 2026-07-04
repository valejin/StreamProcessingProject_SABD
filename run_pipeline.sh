#!/usr/bin/env bash
# =============================================================================
# run_pipeline.sh — Avvia l'intero stack Stream Processing SABD
#
# Uso:
#   bash run_pipeline.sh q1              → lancia Query1Job + metriche, parallelism=1
#   bash run_pipeline.sh q2    4         → lancia Query2Job + metriche, parallelism=4
#   bash run_pipeline.sh all   2         → lancia Q1 + Q2 + metriche, parallelism=2
#
# Il parallelism (secondo argomento, opzionale, default 1) viene passato
# come 'flink run -p N': sovrascrive parallelism.default del cluster solo
# per questo job, senza toccare docker-compose.yml. Utile per l'esperimento
# di impatto del parallelismo richiesto dalla traccia. Combina con
# TIME_SCALE_FACTOR per l'accelerazione del producer, es.:
#   TIME_SCALE_FACTOR=360000 bash run_pipeline.sh q1 4
#
# Fasi eseguite automaticamente:
#   1. Reset InfluxDB (stop/rm container + volume)
#   2. Build Maven (mvn clean package -DskipTests)
#   3. Upload JAR nel container Flink
#   4. Submit job Flink in modalità detached (-d)
#   5. Attesa che il job sia RUNNING (polling REST, max 60s)
#   6. Avvio poll_flink_throughput.sh in background
#   7. Lancio kafka_producer.py
#   8. Attesa 60s dopo fine producer → cancellazione job Flink
#   9. Stop polling REST
#  10. Esecuzione compare_metrics.py (confronto canali)
# =============================================================================

set -euo pipefail

# ── Colori ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GRN='\033[0;32m'; YLW='\033[1;33m'
BLU='\033[0;34m'; CYN='\033[0;36m'; RST='\033[0m'

log()  { echo -e "${BLU}[$(date +%H:%M:%S)]${RST} $*"; }
ok()   { echo -e "${GRN}[$(date +%H:%M:%S)] ✓ $*${RST}"; }
warn() { echo -e "${YLW}[$(date +%H:%M:%S)] ⚠ $*${RST}"; }
die()  { echo -e "${RED}[$(date +%H:%M:%S)] ✗ ERRORE: $*${RST}" >&2; exit 1; }
sep()  { echo -e "${CYN}$(printf '─%.0s' {1..72})${RST}"; }

# ── Configurazione ────────────────────────────────────────────────────────────
QUERY="${1:-}"
[[ "$QUERY" =~ ^(q1|q2|all)$ ]] \
    || { echo "Uso: bash run_pipeline.sh [q1|q2|all] [parallelism]"; exit 1; }

# Parallelismo del job Flink (sovrascrive parallelism.default del cluster
# solo per questo job, via 'flink run -p N'). Default 1 = comportamento
# invariato rispetto a prima di questa modifica.
PARALLELISM="${2:-1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"
DOCKER_DIR="$PROJECT_ROOT/docker"
PRODUCER_SCRIPT="$PROJECT_ROOT/producer/kafka_producer.py"
POLL_SCRIPT="$PROJECT_ROOT/benchmark/poll_flink_throughput.sh"
COMPARE_SCRIPT="$PROJECT_ROOT/benchmark/compare_metrics.py"

FLINK_CONTAINER="flink-jobmanager"
JAR_LOCAL="$PROJECT_ROOT/target/SreamProcessingProject_SABD-1.0-SNAPSHOT.jar"
JAR_REMOTE="/opt/flink/usrlib/SreamProcessingProject_SABD-1.0-SNAPSHOT.jar"

KAFKA_BROKER="${KAFKA_BROKER:-kafka:29092}"
FLINK_URL="${FLINK_URL:-http://localhost:8081}"
RESULTS_DIR="${RESULTS_DIR:-$PROJECT_ROOT/Results}"

CLASS_Q1="it.uniroma2.sabd.query1.Query1Job"
CLASS_Q2="it.uniroma2.sabd.query2.Query2Job"

# Tempi di attesa
FLINK_JOB_READY_TIMEOUT=60   # secondi max per attendere che il job sia RUNNING
FLINK_JOB_READY_POLL=3       # intervallo polling stato job (secondi)
POLL_INTERVAL=5               # intervallo campionamento metriche REST (secondi)
SHUTDOWN_WAIT=60              # secondi dopo producer → cancellazione job

# Array dei JobID lanciati in questa sessione
FLINK_JOB_IDS=()

# PID del processo di polling in background
POLL_PID=""

# ── Funzioni utilità ──────────────────────────────────────────────────────────

# Sottomette un job Flink in modalità detached e cattura il JobID
run_flink_job() {
    local label="$1"
    local class="$2"
    log "Submitting ${label} (parallelism=${PARALLELISM})..."
    local output
    output=$(docker exec \
        -e KAFKA_BROKER="${KAFKA_BROKER}" \
        -e INFLUXDB_ENABLED="true" \
        "${FLINK_CONTAINER}" \
        flink run -d \
            -p "${PARALLELISM}" \
            -c "${class}" \
            "${JAR_REMOTE}" 2>&1) \
        || die "Submit ${label} fallito:\n$output"

    local job_id
    job_id=$(echo "$output" | grep -oP '(?<=JobID )[a-f0-9]+' || true)
    if [[ -z "$job_id" ]]; then
        warn "JobID non estratto dall'output. Output completo:"
        echo "$output"
    else
        FLINK_JOB_IDS+=("$job_id")
        ok "${label} inviato — JobID: ${job_id}"
    fi
}

# Attende che un job specifico sia in stato RUNNING (polling REST)
wait_for_job_running() {
    local job_id="$1"
    local label="$2"
    local elapsed=0

    log "Attendo che ${label} (${job_id:0:8}...) sia RUNNING..."
    while [[ $elapsed -lt $FLINK_JOB_READY_TIMEOUT ]]; do
        local state
        state=$(curl -sf "$FLINK_URL/jobs/$job_id" \
            | python3 -c "import sys,json; print(json.load(sys.stdin).get('state','UNKNOWN'))" \
            2>/dev/null) || state="UNKNOWN"

        if [[ "$state" == "RUNNING" ]]; then
            ok "${label} è RUNNING dopo ${elapsed}s."
            return 0
        elif [[ "$state" == "FAILED" || "$state" == "CANCELED" ]]; then
            die "${label} è passato in stato ${state} — controlla i log Flink."
        fi

        sleep "$FLINK_JOB_READY_POLL"
        elapsed=$(( elapsed + FLINK_JOB_READY_POLL ))
        log "  ... stato: ${state} (${elapsed}s / ${FLINK_JOB_READY_TIMEOUT}s)"
    done

    warn "Timeout: ${label} non è ancora RUNNING dopo ${FLINK_JOB_READY_TIMEOUT}s. Procedo comunque."
}

# Avvia il polling REST in background per una query specifica
start_polling() {
    local query="$1"
    if [[ ! -f "$POLL_SCRIPT" ]]; then
        warn "poll_flink_throughput.sh non trovato in $POLL_SCRIPT — polling saltato."
        return
    fi

    log "Avvio polling metriche REST (ogni ${POLL_INTERVAL}s) in background..."
    FLINK_URL="$FLINK_URL" \
    OUTPUT_DIR="$RESULTS_DIR" \
    bash "$POLL_SCRIPT" "$query" "$POLL_INTERVAL" &
    POLL_PID=$!
    ok "Polling avviato (PID: ${POLL_PID})"
}

# Ferma il polling REST se ancora attivo
stop_polling() {
    if [[ -n "$POLL_PID" ]] && kill -0 "$POLL_PID" 2>/dev/null; then
        log "Fermo polling REST (PID: ${POLL_PID})..."
        kill "$POLL_PID" 2>/dev/null || true
        wait "$POLL_PID" 2>/dev/null || true
        ok "Polling fermato."
    fi
}

# Cancella tutti i job Flink raccolti in FLINK_JOB_IDS
cancel_flink_jobs() {
    if [[ ${#FLINK_JOB_IDS[@]} -eq 0 ]]; then
        warn "Nessun JobID da cancellare."
        return
    fi
    log "Cancellazione job Flink..."
    for jid in "${FLINK_JOB_IDS[@]}"; do
        log "  Cancello job ${jid:0:8}..."
        docker exec "${FLINK_CONTAINER}" flink cancel "$jid" 2>/dev/null \
            && ok "  Job ${jid:0:8} cancellato." \
            || warn "  Job ${jid:0:8} già terminato o non trovato (normale)."
    done
}

# Trap: cleanup in caso di Ctrl+C o errore
cleanup() {
    echo ""
    warn "Interruzione rilevata — cleanup in corso..."
    stop_polling
    cancel_flink_jobs
    exit 1
}
trap cleanup INT TERM

# =============================================================================
# INIZIO PIPELINE
# =============================================================================

sep
echo -e "${CYN}  SABD Project 2 — Stream Processing Pipeline  [${QUERY^^}]${RST}"
sep

# ── STEP 1: Reset InfluxDB ────────────────────────────────────────────────────
sep
log "[1/9] Reset InfluxDB..."

cd "$DOCKER_DIR"
docker compose stop influxdb 2>/dev/null || true
docker compose rm -f influxdb 2>/dev/null || true
docker volume rm docker_influxdb-data 2>/dev/null \
    || warn "Volume docker_influxdb-data non trovato (prima esecuzione?)."
docker compose up -d influxdb
ok "InfluxDB ricreato."

# ── STEP 2: Build Maven ───────────────────────────────────────────────────────
sep
log "[2/9] Build Maven..."
cd "$PROJECT_ROOT"
mvn clean package -DskipTests -q \
    || die "Build Maven fallita."
[[ -f "$JAR_LOCAL" ]] || die "JAR non trovato: $JAR_LOCAL"
ok "Build completata → $(basename "$JAR_LOCAL")"

# ── STEP 3: Upload JAR nel container Flink ────────────────────────────────────
sep
log "[3/9] Upload JAR nel container Flink..."

# Crea la directory usrlib se non esiste
docker exec "${FLINK_CONTAINER}" mkdir -p /opt/flink/usrlib

# Usa REST upload (più affidabile di docker cp da WSL2 su /mnt/c/)
JAR_UPLOAD_ID=$(curl -sf \
    -X POST \
    -H "Expect:" \
    -F "jarfile=@${JAR_LOCAL}" \
    "$FLINK_URL/jars/upload" \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('filename',''))" \
    2>/dev/null) || JAR_UPLOAD_ID=""

if [[ -n "$JAR_UPLOAD_ID" ]]; then
    ok "JAR caricato via REST: $JAR_UPLOAD_ID"
    # Aggiorna JAR_REMOTE con il path restituito dalla REST API
    JAR_REMOTE="$JAR_UPLOAD_ID"
else
    # Fallback: docker cp (funziona se il path locale non è su /mnt/c/)
    warn "Upload REST fallito — provo con docker cp..."
    docker cp "$JAR_LOCAL" "${FLINK_CONTAINER}:${JAR_REMOTE}" \
        || die "docker cp fallito. Carica il JAR manualmente via Web UI ($FLINK_URL)."
    ok "JAR copiato via docker cp."
fi

# ── STEP 4: Submit job/i Flink ────────────────────────────────────────────────
sep
log "[4/9] Submit job Flink (modalità detached)..."

case "$QUERY" in
    q1)  run_flink_job "Query1Job" "$CLASS_Q1" ;;
    q2)  run_flink_job "Query2Job" "$CLASS_Q2" ;;
    all) run_flink_job "Query1Job" "$CLASS_Q1"
         run_flink_job "Query2Job" "$CLASS_Q2" ;;
esac

# ── STEP 5: Attesa job RUNNING (polling REST) ─────────────────────────────────
sep
log "[5/9] Attesa che i job siano RUNNING..."

# Piccola pausa iniziale per dare tempo a Flink di registrare il job
sleep 4

for i in "${!FLINK_JOB_IDS[@]}"; do
    label="Job $((i+1)) (${FLINK_JOB_IDS[$i]:0:8}...)"
    wait_for_job_running "${FLINK_JOB_IDS[$i]}" "$label"
done

# ── STEP 6: Avvio polling metriche REST in background ─────────────────────────
sep
log "[6/9] Avvio polling metriche interne Flink..."

# Determina quale query passare al poller
# (se all, usa q1 come riferimento — Q2 ha vertex ID diversi, gestito separatamente)
POLL_QUERY="$QUERY"
[[ "$QUERY" == "all" ]] && POLL_QUERY="q1"

start_polling "$POLL_QUERY"

# Breve pausa per dare al poller il tempo di fare il primo campione
sleep 2

# ── STEP 7: Lancio Kafka producer ─────────────────────────────────────────────
sep
log "[7/9] Lancio Kafka producer..."
[[ -f "$PRODUCER_SCRIPT" ]] || die "Producer non trovato: $PRODUCER_SCRIPT"

python3 "$PRODUCER_SCRIPT"
ok "Producer completato — tutti gli eventi inviati su Kafka."

# ── STEP 8: Attesa + cancellazione job ────────────────────────────────────────
sep

# Ferma il polling REST subito dopo la fine del producer: la sessione
# campionata (usata per calcolare duration_s e quindi throughput) deve
# coprire solo la fase attiva di invio dati, non l'attesa a vuoto che segue.
# Il countdown sotto resta invariato nello scopo — dare tempo a Flink di
# chiudere le ultime finestre prima di cancellare il job — ma ora è un
# problema separato dalla misurazione, non più mescolato con essa.
stop_polling

log "[8/9] Producer terminato. Attendo ${SHUTDOWN_WAIT}s prima di cancellare i job..."
log "      (il tempo consente alle ultime finestre di essere processate)"

# Countdown visuale ogni 10 secondi
for remaining in $(seq $SHUTDOWN_WAIT -10 10); do
    sleep 10
    log "  ... ${remaining}s rimanenti prima della cancellazione"
done
sleep $(( SHUTDOWN_WAIT % 10 ))   # resto eventuale

# Cancella i job
cancel_flink_jobs

# ── STEP 9: Confronto metriche (canale interno vs esterno) ───────────────────
sep
log "[9/9] Confronto metriche (canale interno Flink vs canale esterno)..."

if [[ ! -f "$COMPARE_SCRIPT" ]]; then
    warn "compare_metrics.py non trovato in $COMPARE_SCRIPT — confronto saltato."
else
    # Determina i path dei CSV in base alla query
    case "$POLL_QUERY" in
        q1)
            EXT_CSV="$RESULTS_DIR/metrics_q1.csv"
            INT_CSV="$RESULTS_DIR/metrics_flink_internal_q1.csv"
            ;;
        q2)
            EXT_CSV="$RESULTS_DIR/metrics_q2.csv"
            INT_CSV="$RESULTS_DIR/metrics_flink_internal_q2.csv"
            ;;
        *)
            EXT_CSV="$RESULTS_DIR/metrics_q1.csv"
            INT_CSV="$RESULTS_DIR/metrics_flink_internal_q1.csv"
            ;;
    esac

    # Attende un momento che i file CSV siano stati flushati su disco
    sleep 3

    if [[ -f "$EXT_CSV" && -f "$INT_CSV" ]]; then
        python3 "$COMPARE_SCRIPT" \
            --ext "$EXT_CSV" \
            --int "$INT_CSV" \
            --out "$RESULTS_DIR" \
            && ok "Confronto completato → $RESULTS_DIR/metrics_comparison_${POLL_QUERY}.csv"
    elif [[ ! -f "$EXT_CSV" ]]; then
        warn "CSV esterno non trovato: $EXT_CSV"
        warn "Assicurati che Q${POLL_QUERY: -1}_METRICS_PATH punti a $EXT_CSV"
    elif [[ ! -f "$INT_CSV" ]]; then
        warn "CSV interno non trovato: $INT_CSV"
        warn "Il polling potrebbe non aver avuto campioni (job troppo breve?)."
    fi
fi

# ── Fine ──────────────────────────────────────────────────────────────────────
sep
ok "Pipeline [${QUERY^^}] completata."
sep
echo ""
echo "  Output disponibili in $RESULTS_DIR:"
ls -lh "$RESULTS_DIR"/*.csv "$RESULTS_DIR"/*.txt 2>/dev/null \
    | awk '{print "    " $NF "  (" $5 ")"}' || true
echo ""

# ── Archiviazione automatica del run (per esperimenti a parallelismo/TSF diversi) ──
# Copia (non sposta) gli output di questo run in una sottocartella etichettata
# con query, parallelism e TIME_SCALE_FACTOR, così run successivi con
# parametri diversi non sovrascrivono i risultati precedenti.
# Segue la stessa convenzione di naming già usata manualmente per gli
# esperimenti su TIME_SCALE_FACTOR (es. Results/q1_3600).
ARCHIVE_TSF="${TIME_SCALE_FACTOR:-86400}"
ARCHIVE_DIR="$RESULTS_DIR/${QUERY}_p${PARALLELISM}_tsf${ARCHIVE_TSF}"
mkdir -p "$ARCHIVE_DIR"
# Copia solo i file della query effettivamente eseguita (tutti i file di
# output/metriche seguono la convenzione *q1*/*q2* nel nome), non l'intero
# contenuto di Results/ — altrimenti file di run precedenti di un'altra
# query, ancora presenti in Results/, finirebbero copiati per errore.
case "$QUERY" in
    q1)  cp "$RESULTS_DIR"/*q1*.csv "$RESULTS_DIR"/*q1*.txt "$ARCHIVE_DIR"/ 2>/dev/null || true ;;
    q2)  cp "$RESULTS_DIR"/*q2*.csv "$RESULTS_DIR"/*q2*.txt "$ARCHIVE_DIR"/ 2>/dev/null || true ;;
    all) cp "$RESULTS_DIR"/*.csv "$RESULTS_DIR"/*.txt "$ARCHIVE_DIR"/ 2>/dev/null || true ;;
esac
ok "Run archiviato in $ARCHIVE_DIR (copia — gli output in $RESULTS_DIR restano invariati)"
echo ""