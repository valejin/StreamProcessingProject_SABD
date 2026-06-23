"""
kafka_producer.py  –  Produttore Kafka per il Progetto 2 SABD
Università degli Studi di Roma Tor Vergata – A.A. 2025/26

Responsabilità del Producer:
  1. LOAD & MERGE   – carica e unisce i 4 CSV mensili del dataset
  2. EVENT TIME     – calcola l'event time da YEAR+MONTH+DAY_OF_MONTH+CRS_DEP_TIME
  3. SORT           – ordina l'intero dataset per event time crescente
  4. REPLAY         – emette gli eventi verso Kafka rispettando le relazioni
                      temporali originali, compresse dal TIME_SCALE_FACTOR

Scelta progettuale – selezione colonne lato Producer:
  Il dataset BTS contiene ~60 colonne. Il Producer legge e trasmette su Kafka
  solo i campi utili alle query Q1 e Q2, per evitare di appesantire il broker
  e il framework di elaborazione con dati irrilevanti, soprattutto considerando
  le risolse limitate nell'ambiente di sviluppo.
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
TIME_SCALE_FACTOR = int(os.getenv("TIME_SCALE_FACTOR", "3600"))

# Dimensione del batch di invio a Kafka (numero di messaggi per flush parziale)
BATCH_LOG_INTERVAL = 50_000

# ─── Colonne da leggere dai CSV ──────────────────────────────────────────────
# Vengono letti solo i campi strettamente necessari per Q1 e Q2, per non
# appesantire Kafka e Flink con le ~50 colonne restanti del dataset BTS
# che non contribuiscono ad alcuna delle query richieste.
COLUMNS_TO_READ = [
    # Identificazione temporale – necessarie per calcolare l'event time
    "YEAR", "MONTH", "DAY_OF_MONTH",
    "CRS_DEP_TIME",       # orario schedulato di partenza (formato HHMM)
    # Compagnia aerea (Q1: filtro su AA, DL, UA, WN)
    "OP_UNIQUE_CARRIER",
    # Aeroporti (Q2: raggruppamento per aeroporto di partenza)
    "ORIGIN_AIRPORT_ID",
    "DEST_AIRPORT_ID",    # incluso per la lista voli in ritardo di Q2
    # Ritardo in partenza (Q1 e Q2: medie, soglie, classifiche)
    "DEP_DELAY",
    # Stato del volo (Q1 e Q2: distinzione completato/cancellato/deviato)
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
    Legge le colonne dichiarate in COLUMNS_TO_READ: sono inclusi tutti i campi
    del dataset utili alle query. Le colonne puramente amministrative del BTS
    (non utili ad alcuna query) vengono escluse già in lettura per contenere
    l'uso di memoria, senza però fare alcun preprocessing semantico.
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

    CRS_DEP_TIME è nel formato HHMM (es. 835 → 08:35, 1425 → 14:25).
    I casi anomali (2400, NaT, valori fuori range) portano allo scarto della riga.

    Usiamo un approccio vettorizzato (pd.to_datetime su colonne separate) invece
    di apply() riga per riga: ~10-20x più veloce su 2.2M righe.
    """
    log.info("Calcolo event time (vettorizzato)...")

    # Estrai ora e minuto da CRS_DEP_TIME in modo vettorizzato
    crs = df["CRS_DEP_TIME"].astype("Int32")
    hour   = (crs // 100).astype("Int16")
    minute = (crs %  100).astype("Int16")

    # Maschera valori anomali: ore >= 24 o minuti >= 60
    valid_mask = (
            df["YEAR"].notna() & df["MONTH"].notna() & df["DAY_OF_MONTH"].notna() &
            crs.notna() &
            (hour >= 0) & (hour < 24) &
            (minute >= 0) & (minute < 60)
    )
    n_invalid = (~valid_mask).sum()
    if n_invalid > 0:
        log.warning("Scartate %d righe con CRS_DEP_TIME anomalo o date mancanti",
                    n_invalid)

    df = df[valid_mask].copy()
    hour   = hour[valid_mask]
    minute = minute[valid_mask]

    # Costruisci datetime vettorizzato
    event_dt = pd.to_datetime({
        "year":  df["YEAR"].astype(int),
        "month": df["MONTH"].astype(int),
        "day":   df["DAY_OF_MONTH"].astype(int),
        "hour":  hour.astype(int),
        "minute": minute.astype(int),
    }, errors="coerce")

    # Eventuali righe che pd.to_datetime non riesce a costruire
    invalid_dt = event_dt.isna()
    if invalid_dt.any():
        log.warning("Ulteriori %d righe scartate per data non costruibile",
                    invalid_dt.sum())
        df = df[~invalid_dt.values]
        event_dt = event_dt[~invalid_dt]

    # Salva l'epoch in millisecondi (int64) – usato per ordinamento e sleep
    df["event_time_ms"] = (
            event_dt.view("int64") // 1_000_000  # ns → ms
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
    Costruisce il messaggio JSON da inviare a Kafka selezionando esplicitamente
    i campi utili alle query Q1 e Q2.

    I valori mancanti (pandas NA/NaN) vengono trasmessi come JSON null: la scelta
    su come trattarli (ignorare la tupla, sostituire con zero, ecc.) è delegata
    a Flink, che la applica in base alla semantica di ciascuna query.

    L'event_time calcolato dal Producer viene incluso in formato ISO 8601,
    pronto per essere assegnato come timestamp Flink tramite WatermarkStrategy.
    """
    def to_python(val):
        """Converte i tipi pandas/numpy in tipi Python nativi serializzabili."""
        if pd.isna(val):
            return None
        # I tipi numpy Int8/Int16/Int32 non sono serializzabili da json.dumps
        if hasattr(val, "item"):
            return val.item()
        return val

    record = {
        "event_time": datetime.utcfromtimestamp(
            row["event_time_ms"] / 1000
        ).strftime("%Y-%m-%dT%H:%M:%S"),
        "airline":          to_python(row["OP_UNIQUE_CARRIER"]),
        "origin_airport_id": to_python(row["ORIGIN_AIRPORT_ID"]),
        "dest_airport_id":  to_python(row["DEST_AIRPORT_ID"]),
        "crs_dep_time":     to_python(row["CRS_DEP_TIME"]),
        "dep_delay":        to_python(row["DEP_DELAY"]),
        "cancelled":        to_python(row["CANCELLED"]),
        "diverted":         to_python(row["DIVERTED"]),
    }

    return json.dumps(record, separators=(",", ":")).encode("utf-8")


# ═══════════════════════════════════════════════════════════════════════════════
# FASE 4 – REPLAY CON ACCELERAZIONE TEMPORALE
# ═══════════════════════════════════════════════════════════════════════════════

def replay(df: pd.DataFrame, producer: KafkaProducer) -> None:
    """
    Emette gli eventi verso Kafka rispettando le relazioni temporali originali,
    compresse dal TIME_SCALE_FACTOR.

    Strategia di timing:
    Invece di calcolare il delta tra due eventi consecutivi (che si azzera spesso
    perché molti voli condividono lo stesso CRS_DEP_TIME), usiamo un'ancora
    assoluta:
      - real_start    = wall-clock al momento della prima emissione
      - et_start_ms   = event time del primo evento
    Per ogni evento:
      - et_offset_ms  = event_time_ms - et_start_ms
      - real_target   = real_start + et_offset_ms / TIME_SCALE_FACTOR
      - sleep(max(0, real_target - now))

    Questo garantisce che le relazioni temporali tra eventi siano rispettate anche
    quando molti eventi hanno lo stesso timestamp (vengono emessi tutti
    "contemporaneamente" senza sleep tra loro).
    """
    n_total = len(df)
    log.info("Inizio replay: %d eventi, TIME_SCALE_FACTOR=%dx", n_total, TIME_SCALE_FACTOR)
    log.info("Durata stimata del replay: ~%.0f secondi",
             (df["event_time_ms"].iloc[-1] - df["event_time_ms"].iloc[0])
             / 1000 / TIME_SCALE_FACTOR)

    et_start_ms  = df["event_time_ms"].iloc[0]
    real_start   = time.monotonic()
    sent         = 0
    errors       = 0

    for _, row in df.iterrows():
        # ── Timing ────────────────────────────────────────────────
        et_offset_ms  = row["event_time_ms"] - et_start_ms
        real_target   = real_start + et_offset_ms / (TIME_SCALE_FACTOR * 1000)
        now           = time.monotonic()
        sleep_time    = real_target - now
        if sleep_time > 0:
            time.sleep(sleep_time)

        # ── Serializzazione & invio ───────────────────────────────
        msg = build_message(row)
        try:
            producer.send(TOPIC_NAME, value=msg)
        except KafkaError as e:
            errors += 1
            log.error("Errore invio messaggio #%d: %s", sent + 1, e)

        sent += 1

        # ── Log periodico ─────────────────────────────────────────
        if sent % BATCH_LOG_INTERVAL == 0:
            et_current = datetime.fromtimestamp(row["event_time_ms"] / 1000)
            lag_s = time.monotonic() - real_target
            log.info(
                "Inviati %7d / %7d  |  event time: %s  |  lag: %+.3f s",
                sent, n_total, et_current, lag_s,
            )

    producer.flush()
    log.info("Replay completato. Inviati: %d  |  Errori: %d", sent, errors)


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
        # Il valore è già bytes (json.dumps().encode()), nessun serializzatore aggiuntivo
        acks=1,           # conferma dal leader – bilanciamento durabilità/throughput
        linger_ms=10,     # mini-batching lato producer per ridurre overhead di rete
        batch_size=65536, # 64 KB per batch (default 16 KB)
        compression_type="lz4",  # compressione leggera, CPU-friendly
        retries=3,
    )

    # ── Fase 3: Replay ───────────────────────────────────────────
    try:
        replay(df, producer)
    finally:
        producer.close()
        log.info("Connessione Kafka chiusa.")


if __name__ == "__main__":
    main()