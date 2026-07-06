# Stream Processing Project — SABD (Progetto 2)

Pipeline di stream processing in tempo reale su dati di voli USA (dataset BTS, gen–apr 2025), sviluppata con **Apache Flink 1.18** (Java 11) per il corso *Sistemi e Architetture per Big Data* (Cardellini, Nardelli) — Università degli Studi di Roma Tor Vergata.

## Architettura

```
kafka_producer.py  →  Kafka ("flights", 1 partizione)  →  Flink  →  CSV + InfluxDB → Grafana
```

- **Producer** (`producer/kafka_producer.py`): replay del dataset BTS su Kafka, con event time compresso da `TIME_SCALE_FACTOR` (default 86400 = 1 giorno di event time → 1 secondo reale) e heartbeat sintetici per evitare buchi di watermark durante le ore notturne senza voli.
- **Flink**: due job indipendenti, entrambi con doppio sink (CSV + InfluxDB).
- **Grafana**: dashboard su InfluxDB per Q1 e Q2.
- **Docker Compose**: Zookeeper, Kafka, InfluxDB, Grafana, Flink JobManager + TaskManager.

## Query implementate

**Query 1 — Monitoraggio compagnie aeree** (`Query1Job`):

Filtro su AA/DL/UA/WN → `keyBy(airline)` → finestre tumbling da 1h → per finestra: numero voli, completati, cancellati, dirottati, ritardo medio in partenza, tasso di cancellazione, tasso di partenze in ritardo.

**Query 2 — Ranking aeroporti per ritardi significativi** (`Query2Job`):

Tre finestre parallele sullo stesso stream: sliding 1h, sliding 6h, e globale cumulativa (`GlobalWindows` + `ContinuousEventTimeTrigger` a 60 min). Per ogni trigger: classifica dei top-10 aeroporti di partenza per numero di ritardi severi (soglia + almeno 30 voli in finestra).

## Metodologia di misurazione

Le metriche sono raccolte su **doppio canale** per validazione incrociata:

| | Canale esterno (custom, CSV)        | Canale interno (Flink REST API) |
|---|-------------------------------------|---|
| Latenza | `outputTime − max(kafkaProduceTime)` | — |
| Throughput | voli processati / durata sessione   | `numRecordsOutPerSecond` (EWMA) |
| Busy % / backpressure | —                                   | `busyTimeMsPerSecond`, `backPressuredTimeMsPerSecond` |

Il LatencyMarker nativo di Flink **non è utilizzato**: non cattura il tempo di buffering in finestra, quindi la latenza end-to-end è misurata con un campo `kafkaProduceTime` iniettato dal producer.

## Struttura del repository

```
src/it/uniroma2/sabd/
  ├── query1/, query2/     job Flink
  ├── model/               FlightEvent (con kafkaProduceTime)
  └── utils/                sorgente Kafka, deserializer, sink InfluxDB, CSV
producer/kafka_producer.py  producer Kafka con replay a event time scalato
benchmark/
  ├── poll_flink_throughput.sh   polling canale interno via REST API
  ├── compare_metrics.py         confronto canale interno vs esterno
  ├── repeat_run.sh, scale_task_slot.sh, scale_taskmanager.sh   esperimenti di scalabilità
docker/docker-compose.yml    stack completo (Kafka, InfluxDB, Grafana, Flink)
grafana/dashboards/          dashboard Q1 e Q2
Results/                     output ed esperimenti di scalabilità (task-slot, TaskManager)
Report/                      report e dichiarazione uso AI
run_pipeline.sh              orchestrazione end-to-end di un run
```

## Come eseguire

```bash
# 1. Avvio stack
cd docker && docker compose up -d

# 2. Creazione topic Kafka (dopo ogni docker compose up)
docker exec kafka kafka-topics --bootstrap-server kafka:29092 \
  --create --topic flights --partitions 1 --replication-factor 1

# 3. Build
mvn clean package -DskipTests

# 4. Esecuzione pipeline completa (build + submit job + producer + metriche)
bash run_pipeline.sh q1              # solo Query 1, parallelism=1
bash run_pipeline.sh q2    4         # solo Query 2, parallelism=4
bash run_pipeline.sh all   2         # entrambe, parallelism=2

# Accelerare il replay (es. 5 min invece di 24h):
TIME_SCALE_FACTOR=360000 bash run_pipeline.sh q1
```

`run_pipeline.sh` gestisce automaticamente: reset InfluxDB, build, upload JAR, submit job, polling metriche REST, avvio producer, cancellazione job e confronto canali (`compare_metrics.py`). Output in `Results/`, archiviati per parallelismo e `TIME_SCALE_FACTOR`.

Web UI Flink: http://localhost:8081

## Stack tecnologico

Apache Flink 1.18 (Java 11) · Kafka (Confluent 7.4) · InfluxDB 2.7 · Grafana · Docker Compose (WSL2) · Python 3 (producer) · Maven

## Autrice

Valentina Jin — progetto singolo.