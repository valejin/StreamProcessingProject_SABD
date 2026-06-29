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

    private static final String METRICS_PATH =
            System.getenv().getOrDefault("Q1_METRICS_PATH", "/results/metrics_q1.csv");

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

        // ── 5. Sink CSV metriche (latenza e throughput) ───────────────────────
        //
        // Una riga per ogni (finestra, airline): una sola misurazione per trigger,
        // indipendente dal numero di voli. La latenza è calcolata come:
        //   latency_ms = System.currentTimeMillis() [al momento dell'output]
        //                - max(kafkaProduceTime)     [tra tutti gli eventi della finestra]
        // Il throughput è in record per minuto di event time:
        //   throughput_rpm = num_flights / window_duration_min  (60 min per Q1)
        // Unità scelta per evitare arrotondamento a 0.00 nelle finestre notturne.
        try (java.io.PrintWriter pw = new java.io.PrintWriter(
                new java.io.FileWriter(METRICS_PATH, false))) {
            pw.println("window_start, window_end, airline, num_flights, latency_ms, throughput_rpm");
        }

        q1Results.map(Query1Job::formatMetricsRow)
                .addSink(new org.apache.flink.streaming.api.functions.sink.RichSinkFunction<String>() {
                    private transient java.io.PrintWriter writer;

                    @Override
                    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
                        writer = new java.io.PrintWriter(new java.io.FileWriter(METRICS_PATH, true));
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
                }).setParallelism(1).name("Metrics CSV Sink Q1");

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

            // Nota: gli heartbeat sono già stati esclusi dal .filter() a monte
            // (airline == null → non passa TARGET_AIRLINES.contains()).
            // Il loop processa solo voli reali AA/DL/UA/WN.
            for (FlightEvent e : events) {
                result.numFlights++;

                // Traccia il massimo kafkaProduceTime tra tutti gli eventi della finestra:
                // rappresenta il momento in cui l'ultimo dato era disponibile in Kafka.
                if (e.getKafkaProduceTime() > result.maxKafkaProduceTime) {
                    result.maxKafkaProduceTime = e.getKafkaProduceTime();
                }

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

            // Imposta il wall-clock di output immediatamente prima di emettere:
            // latency_ms = outputTime - maxKafkaProduceTime
            result.outputTime = System.currentTimeMillis();
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

    /**
     * Formatta una riga per il CSV delle metriche di Q1.
     * Una riga per (finestra, airline): non dipende dal numero di voli nel risultato.
     *
     * latency_ms   = -1 se kafkaProduceTime non disponibile (retrocompatibilità).
     * throughput_rpm usa 4 decimali: anche 1 volo / 60 min = 0.0167, distinguibile da 0.
     */
    private static String formatMetricsRow(Q1WindowResult r) {
        return String.join(", ",
                CsvOutputFormatter.formatTimestamp(r.windowStart),
                CsvOutputFormatter.formatTimestamp(r.windowEnd),
                r.airline,
                String.valueOf(r.numFlights),
                String.valueOf(r.getLatencyMs()),
                String.format("%.4f", r.getThroughputRpm())
        );
    }
}