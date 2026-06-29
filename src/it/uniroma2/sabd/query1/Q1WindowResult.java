package it.uniroma2.sabd.query1;

/**
 * Accumulatore per la finestra tumbling di Q1.
 *
 * Viene aggiornato evento per evento all'interno della ProcessWindowFunction
 * e poi serializzato in una riga CSV al momento della chiusura della finestra.
 *
 * Campi separati per completati/cancellati/deviati invece di derivarli
 * a posteriori: più leggibile e più efficiente (nessun ricalcolo alla fine).
 */
public class Q1WindowResult {

    /** Epoch ms del bordo sinistro della finestra (incluso). */
    public long windowStart;

    /** Epoch ms del bordo destro della finestra (escluso). */
    public long windowEnd;

    public String airline;

    /** Numero totale di voli nella finestra (completed + cancelled + diverted). */
    public int numFlights;

    /** Voli né cancellati né deviati. */
    public int completed;

    /** Voli cancellati. */
    public int cancelled;

    /** Voli deviati. */
    public int diverted;

    /**
     * Somma cumulativa di DEP_DELAY per i voli non cancellati (completed + diverted).
     * Divisa per depDelayCount alla fine per ottenere la media.
     *
     * Nota: la traccia dice "dep_delay_mean considerando solo i voli non cancellati",
     * quindi includiamo anche i deviati nel calcolo della media.
     */
    public double depDelaySum;

    /**
     * Numero di voli non cancellati con DEP_DELAY non null.
     * Serve come denominatore per la media: non tutti i voli non cancellati
     * hanno necessariamente un valore di DEP_DELAY nel dataset.
     */
    public int depDelayCount;

    /**
     * Numero di voli non cancellati con DEP_DELAY > 15 minuti.
     * Usato per calcolare late_departure_rate = lateCount / (numFlights - cancelled).
     */
    public int lateCount;

    /**
     * Massimo wall-clock (epoch ms) tra i kafkaProduceTime di tutti gli eventi
     * della finestra. Rappresenta il momento in cui l'ultimo dato necessario
     * per questa finestra era disponibile nel sistema (prima di Flink).
     *
     * Usato per calcolare la latenza end-to-end:
     *   latency_ms = outputTime - maxKafkaProduceTime
     * dove outputTime = System.currentTimeMillis() al momento dell'output.
     *
     * Valore 0 se nessun evento aveva kafkaProduceTime valorizzato
     * (retrocompatibilità con messaggi prodotti prima dell'aggiunta del campo).
     */
    public long maxKafkaProduceTime;

    /**
     * Wall-clock (epoch ms) del momento in cui la ProcessWindowFunction
     * ha emesso questo risultato. Impostato alla fine di process().
     * Combinato con maxKafkaProduceTime dà la latenza end-to-end.
     */
    public long outputTime;

    public Q1WindowResult() {}

    // ─── Metriche derivate (calcolate alla chiusura della finestra) ───────────

    /**
     * Ritardo medio in partenza per i voli non cancellati.
     * Restituisce Double.NaN se non ci sono voli validi (evita divisione per zero).
     */
    public double getDepDelayMean() {
        return depDelayCount > 0 ? depDelaySum / depDelayCount : Double.NaN;
    }

    /**
     * Tasso di cancellazione: % voli cancellati sul totale.
     * Restituisce 0.0 se numFlights == 0.
     */
    public double getCancellationRate() {
        return numFlights > 0 ? (100.0 * cancelled / numFlights) : 0.0;
    }

    /**
     * Tasso di partenze in ritardo: % voli non cancellati con DEP_DELAY > 15 min.
     * Denominatore = voli non cancellati = completed + diverted.
     * Restituisce 0.0 se non ci sono voli non cancellati.
     */
    public double getLateDepartureRate() {
        int nonCancelled = completed + diverted;
        return nonCancelled > 0 ? (100.0 * lateCount / nonCancelled) : 0.0;
    }

    /**
     * Latenza end-to-end in millisecondi.
     * outputTime - maxKafkaProduceTime: tempo reale tra l'invio dell'ultimo
     * evento su Kafka e l'emissione del risultato da parte di Flink.
     * Restituisce -1 se maxKafkaProduceTime non è disponibile (valore 0).
     */
    public long getLatencyMs() {
        return maxKafkaProduceTime > 0 ? outputTime - maxKafkaProduceTime : -1L;
    }

    /**
     * Throughput in record per minuto di event time.
     * numFlights / durata_finestra_in_minuti (60 min per la tumbling 1h di Q1).
     *
     * Unità scelta al posto di record/s per evitare valori inferiori a 0.01
     * nelle finestre notturne a bassa densità di voli, che verrebbero arrotondati
     * a 0.00 con soli 2 decimali. Con record/min anche il caso peggiore
     * (1 volo / 60 min = 0.0167) rimane distinguibile da zero.
     */
    public double getThroughputRpm() {
        long durationMs = windowEnd - windowStart;
        double durationMin = durationMs / 60_000.0;
        return durationMin > 0 ? numFlights / durationMin : 0.0;
    }
}