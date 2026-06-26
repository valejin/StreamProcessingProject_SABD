package it.uniroma2.sabd.query2;

import java.io.Serializable;
import java.util.List;

/**
 * Rappresenta una singola riga dell'output di Q2:
 * una entry del ranking top-10 per una determinata finestra temporale.
 *
 * Schema CSV:
 *   ts, rank, origin_airport_id, num_flights, severe_delays,
 *   dep_delay_mean, dep_delay_max, delayed_flights
 */
public class Q2RankedEntry implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Epoch ms dell'inizio della finestra (campo "ts" nel CSV). */
    public long windowStart;

    public long influxTs;      // per InfluxDB (solo global window)

    /** Posizione nella classifica (1 = primo). */
    public int rank;

    public int originAirportId;
    public int numFlights;
    public int severeDelays;
    public double depDelayMean;
    public double depDelayMax;
    public List<Q2AirportStats.DelayedFlight> delayedFlights;

    public Q2RankedEntry() {}

    public Q2RankedEntry(long windowStart, int rank,
                         int originAirportId, int numFlights, int severeDelays,
                         double depDelayMean, double depDelayMax,
                         List<Q2AirportStats.DelayedFlight> delayedFlights) {
        this.windowStart     = windowStart;
        this.rank            = rank;
        this.originAirportId = originAirportId;
        this.numFlights      = numFlights;
        this.severeDelays    = severeDelays;
        this.depDelayMean    = depDelayMean;
        this.depDelayMax     = depDelayMax;
        this.delayedFlights  = delayedFlights;
    }
}