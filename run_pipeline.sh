#!/usr/bin/env bash
# =============================================================================
# run_pipeline.sh — Avvia l'intero stack Stream Processing SABD
# Uso:
#   bash run_pipeline.sh q1       → reset InfluxDB, build, deploy, lancia solo Q1 + producer
#   bash run_pipeline.sh q2       → reset InfluxDB, build, deploy, lancia solo Q2 + producer
#   bash run_pipeline.sh all      → reset InfluxDB, build, deploy, lancia Q1 + Q2 + producer
# =============================================================================

set -euo pipefail

# ── Parametro query ──────────────────────────────────────────────────────────
QUERY="${1:-}"   # q1 | q2 | all

usage() {
  echo -e "Uso: bash run_pipeline.sh <query>"
  echo -e "  query: q1 | q2 | all"
  exit 1
}

case "$QUERY" in
  q1|q2|all) ;;   # valori accettati
  *) usage ;;
esac

# ── Colori ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

log()  { echo -e "${CYAN}[$(date '+%H:%M:%S')]${NC} $*"; }
ok()   { echo -e "${GREEN}[$(date '+%H:%M:%S')] ✔ $*${NC}"; }
warn() { echo -e "${YELLOW}[$(date '+%H:%M:%S')] ⚠ $*${NC}"; }
die()  { echo -e "${RED}[$(date '+%H:%M:%S')] ✘ ERRORE: $*${NC}" >&2; exit 1; }

# ── Percorsi ─────────────────────────────────────────────────────────────────
PROJECT_ROOT="/mnt/c/Users/Valen/Desktop/SreamProcessingProject_SABD/SreamProcessingProject_SABD"
DOCKER_DIR="${PROJECT_ROOT}/docker"
JAR_NAME="SreamProcessingProject_SABD-1.0-SNAPSHOT.jar"
JAR_LOCAL="${PROJECT_ROOT}/target/${JAR_NAME}"
JAR_REMOTE="/opt/flink/jobs/${JAR_NAME}"

FLINK_CONTAINER="flink-jobmanager"
KAFKA_BROKER="kafka:29092"

Q1_CLASS="it.uniroma2.sabd.query1.Query1Job"
Q2_CLASS="it.uniroma2.sabd.query2.Query2Job"

FLINK_WAIT_SECONDS=15

# ── Controlli prerequisiti ───────────────────────────────────────────────────
echo -e "\n${BOLD}════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}   SABD Stream Processing — avvio pipeline [${QUERY^^}]  ${NC}"
echo -e "${BOLD}════════════════════════════════════════════════════${NC}\n"

command -v docker  >/dev/null 2>&1 || die "docker non trovato nel PATH"
command -v mvn     >/dev/null 2>&1 || die "mvn non trovato nel PATH"
command -v python3 >/dev/null 2>&1 || die "python3 non trovato nel PATH"

[[ -d "$DOCKER_DIR" ]]   || die "Directory docker non trovata: $DOCKER_DIR"
[[ -f "$PROJECT_ROOT/pom.xml" ]] || die "pom.xml non trovato in: $PROJECT_ROOT"

# ── Step 1: Reset InfluxDB ───────────────────────────────────────────────────
echo -e "\n${BOLD}[1/5] Reset InfluxDB${NC}"
log "Fermando il container influxdb..."
docker compose -f "${DOCKER_DIR}/docker-compose.yml" stop influxdb

log "Rimuovendo il container influxdb..."
docker compose -f "${DOCKER_DIR}/docker-compose.yml" rm -f influxdb

log "Eliminando il volume docker_influxdb-data..."
docker volume rm docker_influxdb-data 2>/dev/null \
  && ok "Volume rimosso." \
  || warn "Volume non trovato (già assente — OK)."

log "Ricreando il container influxdb..."
docker compose -f "${DOCKER_DIR}/docker-compose.yml" up -d influxdb
ok "InfluxDB riavviato."

# ── Step 2: Build Maven ──────────────────────────────────────────────────────
echo -e "\n${BOLD}[2/5] Build Maven${NC}"
log "Esecuzione di: mvn clean package -DskipTests"
cd "$PROJECT_ROOT"
mvn clean package -DskipTests
ok "Build completata → ${JAR_LOCAL}"

[[ -f "$JAR_LOCAL" ]] || die "JAR non trovato dopo la build: ${JAR_LOCAL}"

# ── Step 3: Copia JAR nel container Flink ───────────────────────────────────
echo -e "\n${BOLD}[3/5] Deploy JAR nel container Flink${NC}"
log "Copio il JAR in ${FLINK_CONTAINER}:${JAR_REMOTE} ..."

# Assicura che la directory di destinazione esista nel container
docker exec "$FLINK_CONTAINER" mkdir -p /opt/flink/jobs

docker cp "${JAR_LOCAL}" "${FLINK_CONTAINER}:${JAR_REMOTE}" \
  || die "docker cp fallito. Il container '${FLINK_CONTAINER}' è in esecuzione?"
ok "JAR copiato correttamente."

# ── Step 4: Lancio job Flink ─────────────────────────────────────────────────
echo -e "\n${BOLD}[4/5] Lancio job Flink (${QUERY^^})${NC}"

run_flink_job() {
  local label="$1"
  local class="$2"
  log "Submitting ${label}..."
  docker exec \
    -e KAFKA_BROKER="${KAFKA_BROKER}" \
    "${FLINK_CONTAINER}" \
    flink run -d \
      -c "${class}" \
      "${JAR_REMOTE}" \
    || die "Lancio ${label} fallito."
  ok "${label} inviato a Flink."
}

case "$QUERY" in
  q1)
    run_flink_job "Query1Job" "${Q1_CLASS}"
    ;;
  q2)
    run_flink_job "Query2Job" "${Q2_CLASS}"
    ;;
  all)
    run_flink_job "Query1Job" "${Q1_CLASS}"
    run_flink_job "Query2Job" "${Q2_CLASS}"
    ;;
esac

# ── Step 5: Attesa inizializzazione + producer ───────────────────────────────
echo -e "\n${BOLD}[5/5] Attesa inizializzazione Flink (${FLINK_WAIT_SECONDS}s) + avvio producer${NC}"
log "Aspetto ${FLINK_WAIT_SECONDS} secondi che JobManager e TaskManager siano pronti..."

for i in $(seq "$FLINK_WAIT_SECONDS" -1 1); do
  printf "\r  ${YELLOW}%2ds rimanenti...${NC}" "$i"
  sleep 1
done
echo ""
ok "Attesa completata."

log "Avvio kafka_producer.py ..."
cd "$PROJECT_ROOT"
python3 producer/kafka_producer.py

# ── Fine ─────────────────────────────────────────────────────────────────────
echo -e "\n${GREEN}${BOLD}════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}${BOLD}   Pipeline avviata con successo!                   ${NC}"
echo -e "${GREEN}${BOLD}════════════════════════════════════════════════════${NC}\n"