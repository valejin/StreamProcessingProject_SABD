package it.uniroma2.sabd.query1;

import it.uniroma2.sabd.model.FlightEvent;
import it.uniroma2.sabd.utils.CsvOutputFormatter;
import it.uniroma2.sabd.utils.FlinkSourceBuilder;
import it.uniroma2.sabd.utils.InfluxDbLineProtocol;
import it.uniroma2.sabd.utils.InfluxDbSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

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
 *     → sink 1  : file CSV  (output invariato rispetto alla versione base)
 *     → sink 2  : InfluxDB  (per Grafana, opzionale)
 *
 * Output schema CSV:
 *   window_start, window_end, airline, num_flights, completed, cancelled,
 *   diverted, dep_delay_mean, cancellation_rate, late_departure_rate
 *
 * Output InfluxDB (measurement q1_metrics):
 *   tag:   airline
 *   fields: num_flights, completed, cancelled, diverted,
 *           dep_delay_mean, cancellation_rate, late_departure_rate
 *   timestamp: windowStart (epoch ms)
 */
public class Query1Job {

    private static final Set<String> TARGET_AIRLINES =
            Set.of("AA", "DL", "UA", "WN");

    private static final String KAFKA_BROKER =
            System.getenv().getOrDefault("KAFKA_BROKER",   "kafka:29092");
    private static final String KAFKA_TOPIC  =
            System.getenv().getOrDefault("KAFKA_TOPIC",    "flights");
    private static final String OUTPUT_PATH  =
            System.getenv().getOrDefault("Q1_OUTPUT_PATH", "/results/q1_output.csv");

    // Flag per abilitare/disabilitare il sink InfluxDB senza ricompilare
    private static final boolean INFLUXDB_ENABLED =
            Boolean.parseBoolean(System.getenv().getOrDefault("INFLUXDB_ENABLED", "true"));

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

        // ── 3. Pipeline Q1 → Q1WindowResult ──────────────────────────────────
        DataStream<Q1WindowResult> q1Results = flights
                .filter(e -> e.getAirline() != null
                        && TARGET_AIRLINES.contains(e.getAirline()))
                .keyBy(FlightEvent::getAirline)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new Q1ProcessWindowFunction());

        // ── 4. Sink CSV (comportamento invariato) ─────────────────────────────
        try (java.io.PrintWriter pw = new java.io.PrintWriter(
                new java.io.FileWriter(OUTPUT_PATH, false))) {
            pw.println("window_start, window_end, airline, num_flights, completed, " +
                    "cancelled, diverted, dep_delay_mean, cancellation_rate, late_departure_rate");
        }

        DataStream<String> csvRows = q1Results.map(Query1Job::formatCsvRow);

        csvRows.addSink(new org.apache.flink.streaming.api.functions.sink.RichSinkFunction<String>() {
            private transient java.io.PrintWriter writer;

            @Override
            public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
                writer = new java.io.PrintWriter(new java.io.FileWriter(OUTPUT_PATH, true));
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
        }).setParallelism(1).name("CSV Sink Q1");

        // ── 5. Sink InfluxDB (opzionale, abilitato tramite env var) ──────────
        if (INFLUXDB_ENABLED) {
            q1Results
                    .map(InfluxDbLineProtocol::fromQ1)
                    .addSink(new InfluxDbSink("q1_metrics"))
                    .setParallelism(1)
                    .name("InfluxDB Sink Q1");
        }

        // ── 6. Esecuzione ─────────────────────────────────────────────────────
        env.execute("SABD Project 2 – Query 1");
    }

    // ─── ProcessWindowFunction ────────────────────────────────────────────────

    public static class Q1ProcessWindowFunction
            extends ProcessWindowFunction<FlightEvent, Q1WindowResult, String, TimeWindow> {

        @Override
        public void process(
                String airline,
                Context context,
                Iterable<FlightEvent> events,
                Collector<Q1WindowResult> out) {

            Q1WindowResult result = new Q1WindowResult();
            result.airline     = airline;
            result.windowStart = context.window().getStart();
            result.windowEnd   = context.window().getEnd();

            for (FlightEvent e : events) {
                result.numFlights++;

                if (e.isCancelled()) {
                    result.cancelled++;
                } else if (e.isDiverted()) {
                    result.diverted++;
                    if (e.getDepDelay() != null) {
                        result.depDelaySum += e.getDepDelay();
                        result.depDelayCount++;
                        if (e.getDepDelay() > 15.0) result.lateCount++;
                    }
                } else {
                    result.completed++;
                    if (e.getDepDelay() != null) {
                        result.depDelaySum += e.getDepDelay();
                        result.depDelayCount++;
                        if (e.getDepDelay() > 15.0) result.lateCount++;
                    }
                }
            }

            out.collect(result);
        }
    }

    // ─── Formattazione CSV ────────────────────────────────────────────────────

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
