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
}