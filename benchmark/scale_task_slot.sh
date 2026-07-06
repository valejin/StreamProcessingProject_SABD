#!/usr/bin/env bash
# =============================================================================
# scale_task_slot.sh — Esperimento di scalabilità a livello di task slot
#
# Lancia in sequenza run_pipeline.sh per una query, facendo variare solo il
# parallelism del job (-p), a parità di TaskManager (1) e TIME_SCALE_FACTOR.
# È il test "task slot level": tutti i subtask paralleli restano nella
# stessa JVM/TaskManager, cambia solo quanti slot di quella JVM il job usa.
#
# Uso:
#   bash benchmark/scale_task_slot.sh q1
#   bash benchmark/scale_task_slot.sh q2 "1 2 4"
#   TIME_SCALE_FACTOR=86400 bash benchmark/scale_task_slot.sh q1
#
# Argomenti:
#   $1  query: q1 | q2                          (obbligatorio)
#   $2  lista parallelism separata da spazi,     (opzionale, default "1 2 4")
#       tra virgolette se più di un valore
#
# Variabili d'ambiente:
#   TIME_SCALE_FACTOR   default 86400 (decisione presa per questo esperimento)
#   MAX_SLOTS           default 4 (deve combaciare con taskmanager.numberOfTaskSlots
#                        nel docker-compose.yml — non modificato per questo test)
#
# Output:
#   Results/scale_task_slot_<query>/p<N>/   → una sottocartella per ogni
#       parallelism testato, con dentro tutti i CSV + summary di quel run
#   Results/scale_task_slot_<query>/all_summaries_scale_q1.txt → i summary di tutti
#       i run concatenati in ordine, con separatori, per confronto rapido
#
# Riusa run_pipeline.sh così com'è (nessuna duplicazione di logica): per
# ogni parallelism chiama 'bash run_pipeline.sh <query> <p>' con
# TIME_SCALE_FACTOR fissato, poi riorganizza l'archivio che run_pipeline.sh
# già produce (Results/<query>_p<N>_tsf<TSF>/) nella struttura richiesta qui.
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
[[ "$QUERY" =~ ^(q1|q2)$ ]] \
    || { echo "Uso: bash scale_task_slot.sh [q1|q2] [\"lista parallelism\"]"; exit 1; }

PARALLELISM_LEVELS="${2:-1 2 4}"
TIME_SCALE_FACTOR="${TIME_SCALE_FACTOR:-86400}"
MAX_SLOTS="${MAX_SLOTS:-4}"

# SCRIPT_DIR = benchmark → la root del progetto è un livello sopra
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RUN_PIPELINE="$PROJECT_ROOT/run_pipeline.sh"
RESULTS_DIR="$PROJECT_ROOT/Results"

[[ -f "$RUN_PIPELINE" ]] || die "run_pipeline.sh non trovato in $RUN_PIPELINE"

EXPERIMENT_DIR="$RESULTS_DIR/scale_task_slot_${QUERY}"
mkdir -p "$EXPERIMENT_DIR"
SUMMARY_ALL="$EXPERIMENT_DIR/all_summaries.txt"
: > "$SUMMARY_ALL"   # svuota/crea il file di riepilogo cumulativo

sep
echo -e "${CYN}  Scale test — task slot level — [${QUERY^^}]${RST}"
echo "  Parallelism da testare : $PARALLELISM_LEVELS"
echo "  TIME_SCALE_FACTOR      : $TIME_SCALE_FACTOR"
echo "  Output finale in       : $EXPERIMENT_DIR"
sep

for P in $PARALLELISM_LEVELS; do
    if (( P > MAX_SLOTS )); then
        warn "parallelism=$P supera MAX_SLOTS=$MAX_SLOTS — il job resterebbe in attesa di risorse. Salto."
        continue
    fi

    sep
    log "Run: query=${QUERY} parallelism=${P} tsf=${TIME_SCALE_FACTOR}"
    sep

    TIME_SCALE_FACTOR="$TIME_SCALE_FACTOR" bash "$RUN_PIPELINE" "$QUERY" "$P" \
        || die "run_pipeline.sh fallito per parallelism=$P — controlla l'output sopra."

    # run_pipeline.sh archivia già in Results/<query>_p<P>_tsf<TSF>/
    SRC_ARCHIVE="$RESULTS_DIR/${QUERY}_p${P}_tsf${TIME_SCALE_FACTOR}"
    DST_DIR="$EXPERIMENT_DIR/p${P}"

    if [[ -d "$SRC_ARCHIVE" ]]; then
        rm -rf "$DST_DIR"
        mv "$SRC_ARCHIVE" "$DST_DIR"
        ok "Risultati p=${P} → $DST_DIR"

        SUMMARY_FILE="$DST_DIR/metrics_summary_${QUERY}.txt"
        if [[ -f "$SUMMARY_FILE" ]]; then
            {
                echo ""
                echo "════════════════════════════════════════════════════════════"
                echo "  PARALLELISM = $P"
                echo "════════════════════════════════════════════════════════════"
                cat "$SUMMARY_FILE"
            } >> "$SUMMARY_ALL"
        else
            warn "Summary non trovato per p=${P}: $SUMMARY_FILE"
        fi
    else
        warn "Archivio atteso non trovato: $SRC_ARCHIVE — salto la riorganizzazione per p=${P}."
    fi
done

sep
ok "Esperimento completato. Risultati in: $EXPERIMENT_DIR"
echo ""
echo "  Struttura prodotta:"
find "$EXPERIMENT_DIR" -maxdepth 2 -type d | sort | sed "s|$EXPERIMENT_DIR|    scale_task_slot_${QUERY}|"
echo ""
echo "  Riepilogo comparativo di tutti i run: $SUMMARY_ALL"
sep