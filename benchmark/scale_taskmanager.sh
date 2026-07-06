#!/usr/bin/env bash
# =============================================================================
# scale_taskmanager.sh — Esperimento di scalabilità a livello di TaskManager
#
# A differenza di scale_task_slot.sh (che varia il parallelism nella STESSA
# JVM/TaskManager, con numberOfTaskSlots fissato), qui si fa variare il
# NUMERO di TaskManager (container/JVM separate, 1 slot ciascuno) tramite
# `docker-compose up --scale`, e per ogni numero di TaskManager si ripete
# R volte la stessa configurazione (query fissa, parallelism = numero di TM)
# per stimare la variabilità, esattamente come fa repeat_run.sh.
#
# Richiede il docker-compose.yml aggiornato in cui:
#   - il servizio flink-taskmanager NON ha più container_name fisso
#   - taskmanager.numberOfTaskSlots: 1
#
# Uso:
#   bash benchmark/scale_taskmanager.sh q1
#   bash benchmark/scale_taskmanager.sh q2 "1 2 4" 3
#   TIME_SCALE_FACTOR=86400 bash benchmark/scale_taskmanager.sh q1 "1 2 4" 3
#
# Argomenti:
#   $1  query: q1 | q2                              (obbligatorio)
#   $2  lista numero di TaskManager, separata        (opzionale, default "1 2 4")
#       da spazi, tra virgolette se più di un valore
#   $3  numero di ripetizioni per ogni configurazione (opzionale, default 3)
#
# Variabili d'ambiente:
#   TIME_SCALE_FACTOR   default 86400 (stessa convenzione degli altri script)
#   COMPOSE_FILE        default ../docker-compose.yml relativo a questo script
#   TM_READY_TIMEOUT    secondi massimi di attesa che i TM si registrino
#                        (default 60)
#
# Output:
#   Results/scale_taskmanager_<query>/tm<N>/run<R>/   → CSV + summary di ogni
#       singola ripetizione
#   Results/scale_taskmanager_<query>/tm<N>/aggregate_summary_tm2.txt → media,
#       stdev, cv% delle R ripetizioni per quel numero di TaskManager
#   Results/scale_taskmanager_<query>/all_summaries_scale_q1.txt → tutti gli
#       aggregate_summary concatenati, per confronto rapido tra tm1/tm2/tm4
#
# Riusa run_pipeline.sh così com'è (nessuna duplicazione di logica di
# submit/polling): per ogni (N, ripetizione) chiama
# 'bash run_pipeline.sh <query> <N>' con TIME_SCALE_FACTOR fissato, e
# riorganizza l'archivio che run_pipeline.sh produce da solo.
# =============================================================================

set -euo pipefail

# --- Percorsi (adattati alla struttura reale del progetto:
#     PROJECT_ROOT/benchmark/scale_taskmanager.sh, PROJECT_ROOT/run_pipeline.sh,
#     PROJECT_ROOT/docker/docker-compose.yml, PROJECT_ROOT/Results/) ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RUN_PIPELINE="$PROJECT_ROOT/run_pipeline.sh"
RESULTS_DIR="$PROJECT_ROOT/Results"
COMPOSE_FILE="${COMPOSE_FILE:-$PROJECT_ROOT/docker/docker-compose.yml}"

TIME_SCALE_FACTOR="${TIME_SCALE_FACTOR:-86400}"
TM_READY_TIMEOUT="${TM_READY_TIMEOUT:-60}"
FLINK_REST="http://localhost:8081"

# --- Colori/log helpers ---
info() { echo -e "\033[1;34m[INFO]\033[0m $*"; }
ok()   { echo -e "\033[1;32m[OK]\033[0m $*"; }
warn() { echo -e "\033[1;33m[WARN]\033[0m $*"; }
die()  { echo -e "\033[1;31m[ERR]\033[0m $*" >&2; exit 1; }
sep()  { echo "────────────────────────────────────────────────────────────"; }

# --- Argomenti ---
QUERY="${1:-}"
[[ "$QUERY" == "q1" || "$QUERY" == "q2" ]] || die "Uso: $0 <q1|q2> [\"lista TM\"] [ripetizioni]"

TM_LIST="${2:-1 2 4}"
REPS="${3:-3}"

[[ -x "$RUN_PIPELINE" ]] || die "run_pipeline.sh non trovato/eseguibile in: $RUN_PIPELINE"
[[ -f "$COMPOSE_FILE" ]] || die "docker-compose.yml non trovato in: $COMPOSE_FILE (imposta COMPOSE_FILE se il path è diverso)"

EXPERIMENT_DIR="$RESULTS_DIR/scale_taskmanager_${QUERY}"
SUMMARY_ALL="$EXPERIMENT_DIR/all_summaries.txt"
mkdir -p "$EXPERIMENT_DIR"
: > "$SUMMARY_ALL"

# -----------------------------------------------------------------------
# Attende che esattamente N TaskManager risultino registrati sul JobManager
# -----------------------------------------------------------------------
wait_for_taskmanagers() {
  local expected="$1"
  local waited=0
  while (( waited < TM_READY_TIMEOUT )); do
    local count
    count=$(curl -s "$FLINK_REST/taskmanagers" 2>/dev/null \
      | grep -o '"id"' | wc -l || echo 0)
    if [[ "$count" -eq "$expected" ]]; then
      ok "TaskManager registrati: $count/$expected"
      return 0
    fi
    sleep 2
    waited=$((waited + 2))
  done
  die "Timeout (${TM_READY_TIMEOUT}s): attesi $expected TaskManager, il JobManager ne vede $count"
}

# -----------------------------------------------------------------------
# Calcola mean/stdev/cv% su una lista di numeri (uno per riga da stdin)
# Pura awk: evita dipendenze esterne (python3 potrebbe non essere nel PATH)
# -----------------------------------------------------------------------
aggregate_stats() {
  local label="$1"
  awk -v label="$label" '
    { sum += $1; sumsq += $1*$1; n++; vals = vals sep $1; sep=", " }
    END {
      if (n == 0) { print label ": nessun dato"; exit }
      mean = sum / n
      if (n > 1) {
        var = (sumsq / n) - (mean * mean)
        if (var < 0) var = 0
        stdev = sqrt(var)
      } else {
        stdev = 0
      }
      cv = (mean != 0) ? (stdev / mean * 100) : 0
      printf "%s: mean=%.3f  stdev=%.3f  cv%%=%.2f  n=%d  vals=[%s]\n", label, mean, stdev, cv, n, vals
    }
  '
}

sep
info "Esperimento scale_taskmanager — query=$QUERY  TM=[$TM_LIST]  ripetizioni=$REPS  TSF=$TIME_SCALE_FACTOR"
sep

for N in $TM_LIST; do
  sep
  info "TaskManager=$N — scaling del servizio flink-taskmanager"

  docker-compose -f "$COMPOSE_FILE" up -d --scale flink-taskmanager="$N" flink-taskmanager \
    || die "docker-compose scale fallito per N=$N"

  wait_for_taskmanagers "$N"

  TM_DIR="$EXPERIMENT_DIR/tm${N}"
  mkdir -p "$TM_DIR"

  for ((r = 1; r <= REPS; r++)); do
    info "TM=$N — ripetizione $r/$REPS"
    RUN_DIR="$TM_DIR/run${r}"
    mkdir -p "$RUN_DIR"

    TIME_SCALE_FACTOR="$TIME_SCALE_FACTOR" bash "$RUN_PIPELINE" "$QUERY" "$N" \
      || die "run_pipeline.sh fallito (TM=$N, run=$r)"

    ARCHIVE="$RESULTS_DIR/${QUERY}_p${N}_tsf${TIME_SCALE_FACTOR}"
    [[ -d "$ARCHIVE" ]] || die "Archivio atteso non trovato: $ARCHIVE (controlla la convenzione di naming di run_pipeline.sh)"

    # sposta il contenuto del run appena fatto dentro run<r>/, invece di
    # sovrascrivere l'archivio unico che run_pipeline.sh produce di suo
    cp -r "$ARCHIVE"/. "$RUN_DIR/"
    ok "Archiviato in: $RUN_DIR"
  done

  # --- Aggregazione delle R ripetizioni per questo N ---
  AGG_FILE="$TM_DIR/aggregate_summary.txt"
  {
    echo "=== TaskManager=$N — aggregato su $REPS ripetizioni (query=$QUERY, TSF=$TIME_SCALE_FACTOR) ==="
    for ((r = 1; r <= REPS; r++)); do
      grep -H "" "$TM_DIR/run${r}/metrics_summary_${QUERY}.txt" 2>/dev/null | sed "s/^/run${r}: /" || true
    done

    THROUGHPUT_VALS=$(for ((r = 1; r <= REPS; r++)); do
      grep -oP 'filter_rps mean\s*:\s*\K[\d.]+' "$TM_DIR/run${r}/metrics_summary_${QUERY}.txt" 2>/dev/null
    done)
    BUSY_VALS=$(for ((r = 1; r <= REPS; r++)); do
      grep -oP 'Busy ms/s mean\s*:\s*\K[\d.]+' "$TM_DIR/run${r}/metrics_summary_${QUERY}.txt" 2>/dev/null
    done)
    LAT_MEDIAN_VALS=$(for ((r = 1; r <= REPS; r++)); do
      grep -oP 'Latenza ms\s+median\s*:\s*\K[\d.]+' "$TM_DIR/run${r}/metrics_summary_${QUERY}.txt" 2>/dev/null
    done)

    echo ""
    echo "$THROUGHPUT_VALS"  | aggregate_stats "throughput_rps"
    echo "$BUSY_VALS"        | aggregate_stats "busy_pct"
    echo "$LAT_MEDIAN_VALS"  | aggregate_stats "latenza_median_ms"
  } > "$AGG_FILE"

  cat "$AGG_FILE"
  sep
  echo "" >> "$SUMMARY_ALL"
  cat "$AGG_FILE" >> "$SUMMARY_ALL"
done

# --- Ripristina 1 solo TaskManager a fine esperimento ---
info "Ripristino flink-taskmanager=1 al termine dell'esperimento"
docker-compose -f "$COMPOSE_FILE" up -d --scale flink-taskmanager=1 flink-taskmanager || warn "Ripristino a 1 TM fallito, controlla manualmente"

sep
ok "Esperimento completato. Risultati in: $EXPERIMENT_DIR"
echo ""
echo "  Struttura prodotta:"
find "$EXPERIMENT_DIR" -maxdepth 2 -type d | sort | sed "s|$EXPERIMENT_DIR|    scale_taskmanager_${QUERY}|"
echo ""
echo "  Riepilogo comparativo di tutti i run: $SUMMARY_ALL"
sep