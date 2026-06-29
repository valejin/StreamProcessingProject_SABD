#!/usr/bin/env bash
# ============================================================
# explore_flink_metrics.sh
#
# SCOPO: scoprire i vertex ID e le metriche disponibili
#        sulla REST API di Flink per il job Q1 o Q2.
#
# COME USARE:
#   1. Avvia il job Flink (q1 o q2)
#   2. Esegui: bash explore_flink_metrics.sh
#   3. Incolla l'output nella chat per procedere con
#      poll_flink_throughput.sh
#
# REQUISITI: curl, python3 (solo stdlib)
# ============================================================

FLINK_URL="${FLINK_URL:-http://localhost:8081}"

echo "============================================"
echo " STEP 1 — Job attivi"
echo "============================================"
curl -s "$FLINK_URL/jobs" | python3 -m json.tool

echo ""
echo "============================================"
echo " STEP 2 — Selezione job RUNNING"
echo "============================================"
JOB_ID=$(curl -s "$FLINK_URL/jobs" \
    | python3 -c "
import sys, json
jobs = json.load(sys.stdin).get('jobs', [])
running = [j for j in jobs if j.get('status') == 'RUNNING']
if running:
    print(running[0]['id'])
" 2>/dev/null)

if [ -z "$JOB_ID" ]; then
    echo "ERRORE: nessun job RUNNING trovato."
    echo "Avvia prima il job con: bash run_pipeline.sh q1  (oppure q2)"
    exit 1
fi
echo "Job ID selezionato: $JOB_ID"

echo ""
echo "============================================"
echo " STEP 3 — Vertex (operatori) del job"
echo "============================================"
JOB_JSON=$(curl -s "$FLINK_URL/jobs/$JOB_ID")
echo "$JOB_JSON" | python3 -c "
import sys, json
data = json.load(sys.stdin)
print(f'Nome job : {data[\"name\"]}')
print(f'Stato    : {data[\"state\"]}')
print()
print(f'{'VERTEX_ID':<35} {'NOME OPERATORE':<45} {'STATUS':<12} {'PARALLELISM'}')
print('-'*105)
for v in data.get('vertices', []):
    print(f'{v[\"id\"]:<35} {v[\"name\"][:43]:<45} {v[\"status\"]:<12} {v[\"parallelism\"]}')
"

echo ""
echo "============================================"
echo " STEP 4 — Metriche throughput per vertex"
echo "============================================"
VERTICES=$(echo "$JOB_JSON" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for v in data.get('vertices', []):
    print(v['id'] + '|||' + v['name'])
")

while IFS='|||' read -r VID VNAME; do
    echo ""
    echo "--- Operatore: $VNAME"
    echo "    Vertex ID: $VID"

    METRICS=$(curl -s "$FLINK_URL/jobs/$JOB_ID/vertices/$VID/metrics")
    echo "    Metriche disponibili (throughput / latency / backpressure):"
    echo "$METRICS" | python3 -c "
import sys, json
metrics = json.load(sys.stdin)
keywords = ['numRecords', 'numBytes', 'busyTimeMsPerSecond',
            'idleTimeMsPerSecond', 'backPressuredTimeMsPerSecond']
relevant = [m['id'] for m in metrics
            if any(k in m['id'] for k in keywords)]
for m in sorted(relevant):
    print(f'        {m}')
if not relevant:
    print('        (nessuna metrica rilevante)')
"
done <<< "$VERTICES"

echo ""
echo "============================================"
echo " STEP 5 — Lettura istantanea numRecordsOutPerSecond"
echo "============================================"
echo ""
echo "$JOB_JSON" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for v in data.get('vertices', []):
    print(v['id'] + '|||' + v['name'])
" | while IFS='|||' read -r VID VNAME; do
    VAL=$(curl -s "$FLINK_URL/jobs/$JOB_ID/vertices/$VID/metrics?get=numRecordsOutPerSecond" \
        | python3 -c "
import sys, json
data = json.load(sys.stdin)
print(data[0]['value'] if data else 'N/D')
" 2>/dev/null)
    printf "  %-45s → %s rec/s\n" "${VNAME:0:43}" "$VAL"
done

echo ""
echo "============================================"
echo " RIEPILOGO — incolla questo output nella chat"
echo "============================================"
echo "JOB_ID=$JOB_ID"
echo "FLINK_URL=$FLINK_URL"