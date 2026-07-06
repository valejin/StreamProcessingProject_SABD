"""
kafka_producer.py  –  Produttore Kafka

Responsabilità del Producer:
  1. LOAD & MERGE   – carica e unisce i 4 CSV mensili del dataset
  2. EVENT TIME     – calcola l'event time da YEAR+MONTH+DAY_OF_MONTH+CRS_DEP_TIME
  3. SORT           – ordina l'intero dataset per event time crescente
  4. REPLAY         – emette gli eventi verso Kafka rispettando le relazioni
                      temporali originali, compresse dal TIME_SCALE_FACTOR
  5. HEARTBEAT      – durante i gap notturni (nessun volo) inserisce messaggi
                      heartbeat fittizi per mantenere il Watermark Flink attivo
                      e garantire il trigger della Global Window ogni 60 min
                      di Event Time anche nelle ore senza traffico aereo.

Scelta progettuale – selezione colonne lato Producer:
  Il dataset BTS contiene ~60 colonne. Il Producer legge e trasmette su Kafka
  solo i campi utili alle query Q1 e Q2, per evitare di appesantire il broker
  e il framework di elaborazione con dati irrilevanti, soprattutto considerando
  le risorse limitate nell'ambiente di sviluppo.
  Il filtraggio semantico (valori null, voli cancellati/deviati, soglie di
  ritardo) è invece delegato interamente a Flink, che lo applica sul flusso
  in ingresso prima di eseguire le aggregazioni.
"""

import os
import glob
import json
import time
import logging
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ─── Configurazione ──────────────────────────────────────────────────────────

KAFKA_BROKER    = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME      = os.getenv("KAFKA_TOPIC",  "flights")

# Directory contenente i 4 CSV mensili (202501_T_ONTIME_REPORTING.csv, ecc.)
DATASET_DIR = os.getenv("DATASET_DIR",
                        "/mnt/c/Users/Valen/Desktop/SreamProcessingProject_SABD/"
                        "SreamProcessingProject_SABD/data/project-1-data")

# Fattore di accelerazione temporale.
# TIME_SCALE_FACTOR = X  →  1 secondo reale corrisponde a X secondi di event time.
# Esempi:
#   3600   → 1 ora di event time = 1 s reale  (tutto il dataset ~120 s reali)
#   360    → 1 ora di event time = 10 s reale (debug: finestre leggibili)
#   86400  → 1 giorno di event time = 1 s reale (benchmark veloce)
TIME_SCALE_FACTOR = int(os.getenv("TIME_SCALE_FACTOR", "86400"))

# Dimensione del batch di invio a Kafka (numero di messaggi per flush parziale)
BATCH_LOG_INTERVAL = 50_000

# ─── Configurazione Heartbeat ─────────────────────────────────────────────────
#
# Strategia: quando il gap di Event Time tra due eventi consecutivi supera
# HEARTBEAT_THRESHOLD_MS, il producer inserisce heartbeat fittizi a intervalli
# di HEARTBEAT_INTERVAL_MS fino a colmare il gap.
#
# Questo mantiene il Watermark Flink attivo durante le ore notturne senza voli,
# garantendo che il ContinuousEventTimeTrigger scatti ogni 60 min di Event Time
# anche in assenza di dati reali.
#
# I messaggi heartbeat contengono il flag "heartbeat": true e vengono filtrati
# da Flink prima delle aggregazioni: non inquinano le statistiche Q1 e Q2.
#
# HEARTBEAT_THRESHOLD_MS: gap minimo di Event Time (ms) che attiva l'heartbeat.
#   Default: 5 minuti. Sotto questa soglia non vale la pena interrompere
#   la sequenza naturale degli eventi (il BoundedOutOfOrderness di 5 min
#   garantisce già il watermark in questi casi).
#
# HEARTBEAT_INTERVAL_MS: distanza tra heartbeat consecutivi (Event Time).
#   Default: 10 minuti. Con trigger ogni 60 min, 6 heartbeat/ora garantiscono
#   avanzamento costante del watermark. Ridurre per finestre più piccole.
HEARTBEAT_THRESHOLD_MS = int(os.getenv("HEARTBEAT_THRESHOLD_MS", str(5 * 60 * 1000)))   # 5 min ET
HEARTBEAT_INTERVAL_MS  = int(os.getenv("HEARTBEAT_INTERVAL_MS",  str(10 * 60 * 1000)))  # 10 min ET

# ─── Colonne da leggere dai CSV ──────────────────────────────────────────────
COLUMNS_TO_READ = [
    "YEAR", "MONTH", "DAY_OF_MONTH",
    "CRS_DEP_TIME",
    "OP_UNIQUE_CARRIER",
    "ORIGIN_AIRPORT_ID",
    "DEST_AIRPORT_ID",
    "DEP_DELAY",
    "CANCELLED",
    "DIVERTED",
]

# ─── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# FASE 1 – LOAD & MERGE
# ═══════════════════════════════════════════════════════════════════════════════

def load_dataset(dataset_dir: str) -> pd.DataFrame:
    """
    Carica e unisce i 4 file CSV mensili presenti in dataset_dir.
    """
    pattern = os.path.join(dataset_dir, "*_T_ONTIME_REPORTING.csv")
    files = sorted(glob.glob(pattern))

    if not files:
        raise FileNotFoundError(
            f"Nessun file CSV trovato in: {dataset_dir}\n"
            f"Pattern cercato: {pattern}"
        )

    log.info("File trovati: %d", len(files))
    for f in files:
        log.info("  → %s  (%.1f MB)", os.path.basename(f),
                 os.path.getsize(f) / 1_048_576)

    chunks = []
    for f in files:
        log.info("Lettura %s ...", os.path.basename(f))
        df_part = pd.read_csv(
            f,
            usecols=COLUMNS_TO_READ,
            dtype={
                "YEAR": "Int16", "MONTH": "Int8", "DAY_OF_MONTH": "Int8",
                "CRS_DEP_TIME": "Int16",
                "CANCELLED": "Int8", "DIVERTED": "Int8",
            },
            low_memory=False,
        )
        chunks.append(df_part)
        log.info("  Lette %d righe", len(df_part))

    df = pd.concat(chunks, ignore_index=True)
    log.info("Dataset unito: %d righe totali", len(df))
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# FASE 2 – EVENT TIME + ORDINAMENTO
# ═══════════════════════════════════════════════════════════════════════════════

def compute_event_time(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcola la colonna 'event_time_ms' (epoch ms, int64) a partire da
    YEAR, MONTH, DAY_OF_MONTH, CRS_DEP_TIME.
    """
    log.info("Calcolo event time (vettorizzato)...")

    crs = df["CRS_DEP_TIME"].astype("Int32")
    hour   = (crs // 100).astype("Int16")
    minute = (crs %  100).astype("Int16")

    valid_mask = (
            df["YEAR"].notna() & df["MONTH"].notna() & df["DAY_OF_MONTH"].notna() &
            crs.notna() &
            (hour >= 0) & (hour < 24) &
            (minute >= 0) & (minute < 60)
    )
    n_invalid = (~valid_mask).sum()
    if n_invalid > 0:
        log.warning("Scartate %d righe con CRS_DEP_TIME anomalo o date mancanti", n_invalid)

    df = df[valid_mask].copy()
    hour   = hour[valid_mask]
    minute = minute[valid_mask]

    event_dt = pd.to_datetime({
        "year":  df["YEAR"].astype(int),
        "month": df["MONTH"].astype(int),
        "day":   df["DAY_OF_MONTH"].astype(int),
        "hour":  hour.astype(int),
        "minute": minute.astype(int),
    }, errors="coerce")

    invalid_dt = event_dt.isna()
    if invalid_dt.any():
        log.warning("Ulteriori %d righe scartate per data non costruibile", invalid_dt.sum())
        df = df[~invalid_dt.values]
        event_dt = event_dt[~invalid_dt]

    df["event_time_ms"] = (
            event_dt.view("int64") // 1_000_000
    ).values

    log.info("Event time calcolato. Righe valide: %d", len(df))
    return df


def sort_by_event_time(df: pd.DataFrame) -> pd.DataFrame:
    """Ordina per event time crescente come richiesto dalla traccia."""
    log.info("Ordinamento per event time...")
    df = df.sort_values("event_time_ms", kind="mergesort").reset_index(drop=True)
    first = datetime.fromtimestamp(df["event_time_ms"].iloc[0]  / 1000)
    last  = datetime.fromtimestamp(df["event_time_ms"].iloc[-1] / 1000)
    log.info("Primo evento: %s  |  Ultimo evento: %s", first, last)
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# FASE 3 – SERIALIZZAZIONE
# ═══════════════════════════════════════════════════════════════════════════════

def build_message(row: dict) -> bytes:
    """
    Costruisce il messaggio JSON da inviare a Kafka per un volo reale.
    Il campo "heartbeat" è assente (o False) per i voli normali.

    Il campo "kafka_produce_time" è il wall-clock (epoch ms UTC) misurato
    immediatamente prima dell'invio: viene usato da Flink per calcolare la
    latenza end-to-end (output_time - kafka_produce_time).
    """
    def to_python(val):
        if pd.isna(val):
            return None
        if hasattr(val, "item"):
            return val.item()
        return val

    record = {
        "event_time": datetime.utcfromtimestamp(
            row["event_time_ms"] / 1000
        ).strftime("%Y-%m-%dT%H:%M:%S"),
        "kafka_produce_time": int(time.time() * 1000),   # wall-clock ms UTC
        "airline":          to_python(row["OP_UNIQUE_CARRIER"]),
        "origin_airport_id": to_python(row["ORIGIN_AIRPORT_ID"]),
        "dest_airport_id":  to_python(row["DEST_AIRPORT_ID"]),
        "crs_dep_time":     to_python(row["CRS_DEP_TIME"]),
        "dep_delay":        to_python(row["DEP_DELAY"]),
        "cancelled":        to_python(row["CANCELLED"]),
        "diverted":         to_python(row["DIVERTED"]),
        # heartbeat assente → Flink lo tratta come False (nessuna modifica al deserializer)
    }

    return json.dumps(record, separators=(",", ":")).encode("utf-8")


def build_heartbeat_message(event_time_ms: int) -> bytes:
    """
    Costruisce un messaggio heartbeat fittizio.

    Il messaggio ha event_time impostato all'orario del gap notturno e il flag
    "heartbeat": true. Flink lo usa esclusivamente per far avanzare il Watermark:
    il filtro `e.isHeartbeat()` lo scarta prima di qualsiasi aggregazione.

    Tutti gli altri campi sono null per chiarezza semantica e per garantire che
    un eventuale bug nel filtro non produca conteggi errati nelle statistiche.

    Il campo "kafka_produce_time" è presente anche per gli heartbeat: sebbene
    non vengano usati nelle aggregazioni, il campo viene comunque impostato per
    uniformità e per evitare eccezioni nel deserializer.
    """
    record = {
        "event_time": datetime.utcfromtimestamp(
            event_time_ms / 1000
        ).strftime("%Y-%m-%dT%H:%M:%S"),
        "kafka_produce_time": int(time.time() * 1000),   # wall-clock ms UTC
        "heartbeat": True,
        "airline":           None,
        "origin_airport_id": None,
        "dest_airport_id":   None,
        "crs_dep_time":      None,
        "dep_delay":         None,
        "cancelled":         0,
        "diverted":          0,
    }
    return json.dumps(record, separators=(",", ":")).encode("utf-8")


# ═══════════════════════════════════════════════════════════════════════════════
# FASE 4 – REPLAY CON ACCELERAZIONE TEMPORALE E HEARTBEAT
# ═══════════════════════════════════════════════════════════════════════════════

def send_with_timing(producer: KafkaProducer,
                     payload: bytes,
                     et_offset_ms: int,
                     real_start: float) -> None:
    """
    Attende il momento corretto (wall-clock) prima di inviare il messaggio,
    in base all'offset di Event Time e al TIME_SCALE_FACTOR.

    Separato in funzione per essere riutilizzato sia dai voli reali sia
    dagli heartbeat senza duplicare la logica di timing.
    """
    real_target = real_start + et_offset_ms / (TIME_SCALE_FACTOR * 1000)
    sleep_time  = real_target - time.monotonic()
    if sleep_time > 0:
        time.sleep(sleep_time)
    producer.send(TOPIC_NAME, value=payload)


def replay(df: pd.DataFrame, producer: KafkaProducer) -> None:
    """
    Emette gli eventi verso Kafka rispettando le relazioni temporali originali,
    compresse dal TIME_SCALE_FACTOR.

    Durante i gap di Event Time superiori a HEARTBEAT_THRESHOLD_MS (default 5 min),
    inserisce heartbeat fittizi a intervalli di HEARTBEAT_INTERVAL_MS (default 10 min)
    per mantenere il Watermark Flink attivo nelle ore notturne senza voli.

    Strategia di timing (invariata rispetto alla versione senza heartbeat):
    - real_start  = wall-clock al momento della prima emissione
    - et_start_ms = event time del primo evento
    Ogni messaggio (reale o heartbeat) attende:
      real_target = real_start + (event_time_ms - et_start_ms) / (TIME_SCALE_FACTOR * 1000)
    """
    n_total = len(df)
    log.info("Inizio replay: %d eventi, TIME_SCALE_FACTOR=%dx", n_total, TIME_SCALE_FACTOR)
    log.info("Durata stimata del replay: ~%.0f secondi",
             (df["event_time_ms"].iloc[-1] - df["event_time_ms"].iloc[0])
             / 1000 / TIME_SCALE_FACTOR)
    log.info("Heartbeat: threshold=%d min ET, interval=%d min ET",
             HEARTBEAT_THRESHOLD_MS // 60_000, HEARTBEAT_INTERVAL_MS // 60_000)

    et_start_ms   = df["event_time_ms"].iloc[0]
    real_start     = time.monotonic()
    sent_real      = 0
    sent_heartbeat = 0
    errors         = 0
    prev_et_ms     = et_start_ms

    for _, row in df.iterrows():
        curr_et_ms   = row["event_time_ms"]
        gap_ms       = curr_et_ms - prev_et_ms

        # ── Inserimento heartbeat se gap supera la soglia ─────────────────────
        if gap_ms > HEARTBEAT_THRESHOLD_MS:
            # Primo heartbeat: HEARTBEAT_INTERVAL_MS dopo l'ultimo evento reale
            hb_et_ms = prev_et_ms + HEARTBEAT_INTERVAL_MS
            while hb_et_ms < curr_et_ms:
                hb_offset_ms = hb_et_ms - et_start_ms
                try:
                    send_with_timing(producer,
                                     build_heartbeat_message(hb_et_ms),
                                     hb_offset_ms,
                                     real_start)
                    sent_heartbeat += 1
                except KafkaError as e:
                    errors += 1
                    log.error("Errore invio heartbeat ET=%s: %s",
                              datetime.utcfromtimestamp(hb_et_ms / 1000), e)
                hb_et_ms += HEARTBEAT_INTERVAL_MS

        # ── Evento reale ──────────────────────────────────────────────────────
        et_offset_ms = curr_et_ms - et_start_ms
        try:
            send_with_timing(producer, build_message(row), et_offset_ms, real_start)
        except KafkaError as e:
            errors += 1
            log.error("Errore invio messaggio #%d: %s", sent_real + 1, e)

        sent_real += 1
        prev_et_ms = curr_et_ms

        # ── Log periodico ─────────────────────────────────────────────────────
        if sent_real % BATCH_LOG_INTERVAL == 0:
            et_current = datetime.fromtimestamp(curr_et_ms / 1000)
            lag_s = time.monotonic() - (real_start + et_offset_ms / (TIME_SCALE_FACTOR * 1000))
            log.info(
                "Inviati %7d reali + %5d heartbeat  |  event time: %s  |  lag: %+.3f s",
                sent_real, sent_heartbeat, et_current, lag_s,
            )

    producer.flush()
    log.info("Replay completato. Reali: %d  |  Heartbeat: %d  |  Errori: %d",
             sent_real, sent_heartbeat, errors)


# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    # ── Fase 1: Load & Merge ──────────────────────────────────────
    df = load_dataset(DATASET_DIR)

    # ── Fase 2: Calcolo event time + ordinamento ──────────────────
    df = compute_event_time(df)
    df = sort_by_event_time(df)

    # ── Connessione a Kafka ───────────────────────────────────────
    log.info("Connessione a Kafka: %s (topic: %s) ...", KAFKA_BROKER, TOPIC_NAME)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        acks=1,
        linger_ms=10,
        batch_size=65536,
        compression_type="lz4",
        retries=3,
    )

    # ── Fase 3: Replay con heartbeat ─────────────────────────────
    try:
        replay(df, producer)
    finally:
        producer.close()
        log.info("Connessione Kafka chiusa.")


if __name__ == "__main__":
    main()