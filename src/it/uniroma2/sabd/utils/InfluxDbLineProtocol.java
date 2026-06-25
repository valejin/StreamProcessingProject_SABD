package it.uniroma2.sabd.utils;

import it.uniroma2.sabd.query1.Q1WindowResult;
import it.uniroma2.sabd.query2.Q2RankedEntry;

/**
 * Costruisce le stringhe in InfluxDB Line Protocol per Q1 e Q2.
 *
 * Line Protocol reference:
 *   measurement[,tag=value ...] field=value[,...] timestamp_ms
 *
 * Tag:    indicizzati, usati per filtrare/raggruppare in Grafana (stringhe).
 * Fields: dati numerici o stringhe non indicizzate.
 * Timestamp: epoch in millisecondi (precision=ms impostato nell'URL di write).
 *
 * Scelta dei tag vs field:
 *   - "airline" e "origin_airport_id" → TAG (usati come dimensioni in Grafana)
 *   - tutte le metriche numeriche      → FIELD
 *   - "rank" in Q2                     → TAG (permette di filtrare per posizione)
 */
public class InfluxDbLineProtocol {

    // ──────────────────────────────────────────────────────────────────────────
    // Query 1
    // ──────────────────────────────────────────────────────────────────────────

    /**
     * Genera la riga Line Protocol per un risultato di Q1.
     *
     * Measurement: q1_metrics
     * Tags:        airline
     * Fields:      num_flights, completed, cancelled, diverted,
     *              dep_delay_mean, cancellation_rate, late_departure_rate
     * Timestamp:   windowStart (epoch ms) → punto temporale del bordo sinistro
     *
     * Esempio output:
     *   q1_metrics,airline=AA num_flights=13i,completed=13i,cancelled=0i,
     *   diverted=0i,dep_delay_mean=7.38,cancellation_rate=0.00,
     *   late_departure_rate=30.77 1735689600000
     */
    public static String fromQ1(Q1WindowResult r) {
        StringBuilder sb = new StringBuilder();

        // measurement + tag
        sb.append("q1_metrics,airline=").append(escapeTag(r.airline));

        // fields
        sb.append(" ");
        sb.append("num_flights=").append(r.numFlights).append("i,");
        sb.append("completed=").append(r.completed).append("i,");
        sb.append("cancelled=").append(r.cancelled).append("i,");
        sb.append("diverted=").append(r.diverted).append("i,");
        sb.append("dep_delay_mean=").append(safeDouble(r.getDepDelayMean())).append(",");
        sb.append("cancellation_rate=").append(r.getCancellationRate()).append(",");
        sb.append("late_departure_rate=").append(r.getLateDepartureRate());

        // timestamp in ms
        sb.append(" ").append(r.windowStart);

        return sb.toString();
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Query 2
    // ──────────────────────────────────────────────────────────────────────────

    /**
     * Genera la riga Line Protocol per una entry del ranking Q2.
     *
     * Measurement: q2_ranking_{windowType}  (es. q2_ranking_1h, q2_ranking_6h, q2_ranking_global)
     * Tags:        rank, origin_airport_id
     * Fields:      num_flights, severe_delays, dep_delay_mean, dep_delay_max
     * Timestamp:   windowStart (epoch ms)
     *
     * Nota: il campo "delayed_flights" (lista) non viene scritto in InfluxDB
     * perché è un tipo non supportato in modo nativo e non ha utilità in Grafana.
     * Rimane solo nell'output CSV.
     *
     * @param r          entry del ranking
     * @param windowType stringa descrittiva del tipo di finestra ("1h", "6h", "global")
     */
    public static String fromQ2(Q2RankedEntry r, String windowType) {
        StringBuilder sb = new StringBuilder();

        // measurement + tags
        sb.append("q2_ranking_").append(windowType);
        sb.append(",rank=").append(r.rank);
        sb.append(",origin_airport_id=").append(r.originAirportId);

        // fields
        sb.append(" ");
        sb.append("num_flights=").append(r.numFlights).append("i,");
        sb.append("severe_delays=").append(r.severeDelays).append("i,");
        sb.append("dep_delay_mean=").append(safeDouble(r.depDelayMean)).append(",");
        sb.append("dep_delay_max=").append(safeDouble(r.depDelayMax));

        // timestamp in ms
        sb.append(" ").append(r.windowStart);

        return sb.toString();
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Utilità private
    // ──────────────────────────────────────────────────────────────────────────

    /**
     * Converte NaN/Infinity in 0.0 (InfluxDB non accetta valori non finiti).
     */
    private static double safeDouble(double v) {
        return (Double.isNaN(v) || Double.isInfinite(v)) ? 0.0 : v;
    }

    /**
     * Escape dei caratteri speciali nei valori dei tag InfluxDB:
     *   spazio, virgola, uguale → preceduti da backslash.
     */
    private static String escapeTag(String value) {
        if (value == null) return "unknown";
        return value.replace(" ", "\\ ")
                    .replace(",", "\\,")
                    .replace("=", "\\=");
    }
}
