package it.uniroma2.sabd.query2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Accumulatore per le statistiche di un singolo aeroporto di partenza
 * all'interno di una finestra temporale.
 *
 * Prodotto dallo Stage 1 (Q2PerAirportFunction, keyBy originAirportId)
 * e consumato dallo Stage 2 (Q2RankingFunction) per costruire il top-10.
 *
 * Campi:
 *  - windowStart   : epoch ms inizio finestra (usato come timestamp nel CSV
 *                    e come event time per assegnare questo oggetto alla
 *                    finestra corretta nello Stage 2)
 *  - numFlights    : voli non cancellati e non deviati (filtro >= 30)
 *  - severeDelays  : voli con DEP_DELAY > 30 min (criterio di ranking)
 *  - depDelaySum/Count : per il calcolo della media
 *  - depDelayMax   : massimo DEP_DELAY osservato
 *  - delayedFlights: lista top-20 voli con ritardo significativo
 */
public class Q2AirportStats implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final int MAX_DELAYED_FLIGHTS = 20;

    /** Epoch ms dell'inizio della finestra. Usato come event time nello Stage 2. */
    public long windowStart;

    public int originAirportId;
    public int numFlights;
    public int severeDelays;
    public double depDelaySum;
    public int depDelayCount;
    public double depDelayMax = Double.NEGATIVE_INFINITY;
    public List<DelayedFlight> delayedFlights = new ArrayList<>();

    public Q2AirportStats() {}

    // ─── Aggiornamento ────────────────────────────────────────────────────────

    /**
     * Aggiunge un volo non cancellato e non deviato all'accumulatore.
     * Chiamato una volta per ogni FlightEvent nella finestra.
     */
    public void addFlight(String airline, Integer destAirportId, Double depDelay) {
        numFlights++;
        if (depDelay != null) {
            depDelaySum += depDelay;
            depDelayCount++;
            if (depDelay > depDelayMax) depDelayMax = depDelay;
            if (depDelay > 30.0) {
                severeDelays++;
                delayedFlights.add(new DelayedFlight(
                        airline != null ? airline : "??",
                        destAirportId != null ? destAirportId : -1,
                        depDelay
                ));
            }
        }
    }

    // ─── Metriche derivate ────────────────────────────────────────────────────

    public double getDepDelayMean() {
        return depDelayCount > 0 ? depDelaySum / depDelayCount : Double.NaN;
    }

    public double getDepDelayMaxSafe() {
        return depDelayCount > 0 ? depDelayMax : Double.NaN;
    }

    /**
     * Restituisce i voli con ritardo significativo ordinati per DEP_DELAY
     * decrescente, troncati a MAX_DELAYED_FLIGHTS.
     */
    public List<DelayedFlight> getTopDelayedFlights() {
        List<DelayedFlight> result = new ArrayList<>(delayedFlights);
        result.sort(Comparator.comparingDouble((DelayedFlight f) -> f.depDelay).reversed());
        if (result.size() > MAX_DELAYED_FLIGHTS) {
            result = new ArrayList<>(result.subList(0, MAX_DELAYED_FLIGHTS));
        }
        return result;
    }

    // ─── Inner class ─────────────────────────────────────────────────────────

    public static class DelayedFlight implements Serializable {
        private static final long serialVersionUID = 1L;

        public String airline;
        public int destAirportId;
        public double depDelay;

        public DelayedFlight() {}

        public DelayedFlight(String airline, int destAirportId, double depDelay) {
            this.airline = airline;
            this.destAirportId = destAirportId;
            this.depDelay = depDelay;
        }

        @Override
        public String toString() {
            return String.format("(%s,%d,%.2f)", airline, destAirportId, depDelay);
        }
    }
}