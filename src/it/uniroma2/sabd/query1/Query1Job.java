package it.uniroma2.sabd.query1;

import it.uniroma2.sabd.model.FlightEvent;
import it.uniroma2.sabd.utils.CsvOutputFormatter;
import it.uniroma2.sabd.utils.FlinkSourceBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Set;

/**
 * Query 1 – Monitoraggio in tempo reale delle principali compagnie aeree.
 *
 * Pipeline:
 *   Kafka ("flights")
 *     → filter  : solo AA, DL, UA, WN
 *     → keyBy   : airline
 *     → window  : TumblingEventTimeWindows(1 ora)
 *     → process : Q1ProcessWindowFunction (calcola le 6 metriche)
 *     → sink    : file CSV
 *
 * Output schema:
 *   window_start, window_end, airline, num_flights, completed, cancelled,
 *   diverted, dep_delay_mean, cancellation_rate, late_departure_rate
 */
public class Query1Job {

    // Compagnie monitorate (Set per lookup O(1))
    private static final Set<String> TARGET_AIRLINES =
            Set.of("AA", "DL", "UA", "WN");

    // Variabili d'ambiente con valori di default per sviluppo locale
    private static final String KAFKA_BROKER =
            System.getenv().getOrDefault("KAFKA_BROKER", "kafka:29092");
    private static final String KAFKA_TOPIC =
            System.getenv().getOrDefault("KAFKA_TOPIC", "flights");
    private static final String OUTPUT_PATH =
            System.getenv().getOrDefault("Q1_OUTPUT_PATH", "/results/q1_output.csv");

    public static void main(String[] args) throws Exception {

        // ── 1. Ambiente Flink ─────────────────────────────────────────────────
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // ── 2. Sorgente Kafka ─────────────────────────────────────────────────
        KafkaSource<FlightEvent> kafkaSource = FlinkSourceBuilder.buildKafkaSource(
                KAFKA_BROKER, KAFKA_TOPIC, "flink-q1-group");

        WatermarkStrategy<FlightEvent> watermarkStrategy =
                FlinkSourceBuilder.buildWatermarkStrategy();

        DataStream<FlightEvent> flights = env
                .fromSource(kafkaSource, watermarkStrategy, "Kafka flights source");

        // ── 3. Pipeline Q1 ────────────────────────────────────────────────────
        DataStream<String> q1Results = flights

                // Filtro: solo le 4 compagnie di interesse
                // I voli con airline null vengono scartati dal contains()
                .filter(e -> e.getAirline() != null
                        && TARGET_AIRLINES.contains(e.getAirline()))

                // Partizionamento per compagnia: ogni partizione gestisce
                // indipendentemente una compagnia → parallelismo efficace
                .keyBy(FlightEvent::getAirline)

                // Finestra tumbling di 1 ora su event time
                .window(TumblingEventTimeWindows.of(Time.hours(1)))

                // Aggregazione: ProcessWindowFunction riceve tutti gli eventi
                // della finestra in un'unica chiamata dopo la chiusura
                .process(new Q1ProcessWindowFunction())

                // Formattazione CSV
                .map(result -> formatCsvRow(result));

        // ── 4. Sink: file CSV ─────────────────────────────────────────────────
        // Scrivi l'header prima dell'esecuzione del job, così è garantito
        // che sia la prima riga del file indipendentemente dalla schedulazione.
        try (java.io.PrintWriter pw = new java.io.PrintWriter(
                new java.io.FileWriter(OUTPUT_PATH, false))) {  // false = overwrite
            pw.println("window_start, window_end, airline, num_flights, completed, " +
                    "cancelled, diverted, dep_delay_mean, cancellation_rate, late_departure_rate");
        }

        // Il job scrive i dati in append (OVERWRITE qui sovrascrive solo dal secondo
        // elemento in poi, ma poiché il file esiste già con l'header, usiamo NO_OVERWRITE
        // per appendere — oppure usiamo direttamente un FileWriter in append nel sink).
        // Soluzione più semplice: sink separato con append manuale.
        q1Results.addSink(new org.apache.flink.streaming.api.functions.sink.RichSinkFunction<String>() {
            private transient java.io.PrintWriter writer;

            @Override
            public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
                writer = new java.io.PrintWriter(new java.io.FileWriter(OUTPUT_PATH, true)); // true = append
            }

            @Override
            public void invoke(String value, Context context) {
                writer.println(value);
                writer.flush();
            }

            @Override
            public void close() {
                if (writer != null) writer.close();
            }
        }).setParallelism(1);


        // ── 5. Esecuzione ─────────────────────────────────────────────────────
        env.execute("SABD Project 2 – Query 1");
    }

    // ─── ProcessWindowFunction ────────────────────────────────────────────────

    /**
     * Riceve tutti gli eventi della finestra [windowStart, windowEnd) per una
     * singola compagnia e calcola le metriche richieste dalla traccia.
     *
     * Complessità: O(n) per n eventi nella finestra – scorre ogni evento una volta.
     */
    public static class Q1ProcessWindowFunction
            extends ProcessWindowFunction<FlightEvent, Q1WindowResult, String, TimeWindow> {

        @Override
        public void process(
                String airline,
                Context context,
                Iterable<FlightEvent> events,
                Collector<Q1WindowResult> out) {

            Q1WindowResult result = new Q1WindowResult();
            result.airline = airline;

            // Imposta i bound temporali della finestra (usati nel CSV)
            TimeWindow window = context.window();
            // Salviamo start/end nel result per passarli al formatter
            // (ProcessWindowFunction non espone direttamente i bound al map successivo,
            // quindi li aggiungiamo al POJO)
            result.windowStart = window.getStart();
            result.windowEnd   = window.getEnd();

            for (FlightEvent e : events) {
                result.numFlights++;

                if (e.isCancelled()) {
                    result.cancelled++;

                } else if (e.isDiverted()) {
                    result.diverted++;
                    // I voli deviati contribuiscono alla media dep_delay
                    // (non sono cancellati → hanno un DEP_DELAY)
                    if (e.getDepDelay() != null) {
                        result.depDelaySum += e.getDepDelay();
                        result.depDelayCount++;
                        if (e.getDepDelay() > 15.0) {
                            result.lateCount++;
                        }
                    }
                    // I voli deviati = voli non cancellati
                    // (la traccia dice "% voli non cancellati con DEP_DELAY > 15 min",
                    // quindi li conto nel numeratore e nel denominatore per il calcolo del rate

                } else {
                    // Volo completato
                    result.completed++;
                    if (e.getDepDelay() != null) {
                        result.depDelaySum += e.getDepDelay();
                        result.depDelayCount++;
                        if (e.getDepDelay() > 15.0) {
                            result.lateCount++;
                        }
                    }
                }
            }

            out.collect(result);
        }
    }

    // ─── Formattazione CSV ────────────────────────────────────────────────────

    /**
     * Produce una riga CSV secondo lo schema della traccia:
     * window_start, window_end, airline, num_flights, completed, cancelled,
     * diverted, dep_delay_mean, cancellation_rate, late_departure_rate
     */
    private static String formatCsvRow(Q1WindowResult r) {
        return String.join(", ",
                CsvOutputFormatter.formatTimestamp(r.windowStart),
                CsvOutputFormatter.formatTimestamp(r.windowEnd),
                r.airline,
                String.valueOf(r.numFlights),
                String.valueOf(r.completed),
                String.valueOf(r.cancelled),
                String.valueOf(r.diverted),
                CsvOutputFormatter.formatDouble(r.getDepDelayMean()),
                CsvOutputFormatter.formatPercent(r.getCancellationRate()),
                CsvOutputFormatter.formatPercent(r.getLateDepartureRate())
        );
    }
}