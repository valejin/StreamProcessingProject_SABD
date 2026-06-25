package it.uniroma2.sabd.query2;

import it.uniroma2.sabd.model.FlightEvent;
import it.uniroma2.sabd.utils.CsvOutputFormatter;
import it.uniroma2.sabd.utils.FlinkSourceBuilder;
import it.uniroma2.sabd.utils.InfluxDbLineProtocol;
import it.uniroma2.sabd.utils.InfluxDbSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;

/**
 * Query 2 – Identificazione in tempo reale degli aeroporti di partenza
 * con il maggior numero di ritardi significativi.
 *
 * Tre finestre temporali (event time):
 *   1h     → SlidingEventTimeWindows(size=1h,  slide=60min)
 *   6h     → SlidingEventTimeWindows(size=6h,  slide=60min)
 *   global → GlobalWindows + ContinuousEventTimeTrigger(60min)
 *
 * Approccio: ProcessAllWindowFunction diretta su windowAll.
 * I duplicati prodotti dai pane interni della sliding window vengono
 * eliminati nel sink CSV tramite un HashSet che tiene traccia delle righe
 * già scritte (deduplicazione basata sul contenuto della riga CSV).
 * Il sink InfluxDB non necessita di deduplicazione: tag identici con lo
 * stesso timestamp sovrascrivono il punto precedente (upsert nativo).
 *
 * Per la finestra global, il timestamp di output è il massimo event time
 * visto tra gli elementi della finestra (più significativo di
 * GlobalWindow.getStart() che vale Long.MIN_VALUE).
 */
public class Query2Job {

    // ── Configurazione ────────────────────────────────────────────────────────
    private static final String KAFKA_BROKER =
            System.getenv().getOrDefault("KAFKA_BROKER", "kafka:29092");
    private static final String KAFKA_TOPIC =
            System.getenv().getOrDefault("KAFKA_TOPIC", "flights");
    private static final String OUTPUT_PATH_1H =
            System.getenv().getOrDefault("Q2_OUTPUT_PATH_1H", "/results/q2_output_1h.csv");
    private static final String OUTPUT_PATH_6H =
            System.getenv().getOrDefault("Q2_OUTPUT_PATH_6H", "/results/q2_output_6h.csv");
    private static final String OUTPUT_PATH_GLOBAL =
            System.getenv().getOrDefault("Q2_OUTPUT_PATH_GLOBAL", "/results/q2_output_global.csv");

    // Flag per abilitare/disabilitare il sink InfluxDB senza ricompilare
    private static final boolean INFLUXDB_ENABLED =
            Boolean.parseBoolean(System.getenv().getOrDefault("INFLUXDB_ENABLED", "true"));

    private static final int MIN_FLIGHTS = 30;

    private static final String CSV_HEADER =
            "ts, rank, origin_airport_id, num_flights, severe_delays, " +
                    "dep_delay_mean, dep_delay_max, delayed_flights";

    public static void main(String[] args) throws Exception {

        // ── 1. Ambiente Flink ─────────────────────────────────────────────────
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // ── 2. Sorgente Kafka ─────────────────────────────────────────────────
        KafkaSource<FlightEvent> kafkaSource = FlinkSourceBuilder.buildKafkaSource(
                KAFKA_BROKER, KAFKA_TOPIC, "flink-q2-group");

        WatermarkStrategy<FlightEvent> watermarkStrategy =
                FlinkSourceBuilder.buildWatermarkStrategy()
                        .withIdleness(Duration.ofSeconds(10));

        DataStream<FlightEvent> flights = env
                .fromSource(kafkaSource, watermarkStrategy, "Kafka flights source Q2");

        // ── 3. Pre-filtro: solo voli completati con originAirportId valido ────
        DataStream<FlightEvent> completedFlights = flights
                .filter(e -> e.isCompleted() && e.getOriginAirportId() != null);

        // ── 4. Finestra 1h sliding (slide 60min) ─────────────────────────────
        DataStream<Q2RankedEntry> ranked1h = completedFlights
                .windowAll(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(60)))
                .process(new Q2SlidingRankingFunction());

        // ── 5. Finestra 6h sliding (slide 60min) ─────────────────────────────
        DataStream<Q2RankedEntry> ranked6h = completedFlights
                .windowAll(SlidingEventTimeWindows.of(Time.hours(6), Time.minutes(60)))
                .process(new Q2SlidingRankingFunction());

        // ── 6. Finestra global (trigger ogni 60min di event time) ─────────────
        DataStream<Q2RankedEntry> rankedGlobal = completedFlights
                .windowAll(GlobalWindows.create())
                .trigger(ContinuousEventTimeTrigger.of(Time.minutes(60)))
                .process(new Q2GlobalRankingFunction());

        // ── 7. Sink CSV con header e deduplicazione ───────────────────────────
        writeWithHeader(ranked1h.map(Query2Job::formatCsvRow),     OUTPUT_PATH_1H);
        writeWithHeader(ranked6h.map(Query2Job::formatCsvRow),     OUTPUT_PATH_6H);
        writeWithHeader(rankedGlobal.map(Query2Job::formatCsvRow), OUTPUT_PATH_GLOBAL);

        // ── 8. Sink InfluxDB (per Grafana) ────────────────────────────────────
        if (INFLUXDB_ENABLED) {
            ranked1h
                    .map(r -> InfluxDbLineProtocol.fromQ2(r, "1h"))
                    .addSink(new InfluxDbSink("q2_ranking_1h"))
                    .setParallelism(1)
                    .name("InfluxDB Sink Q2 1h");

            ranked6h
                    .map(r -> InfluxDbLineProtocol.fromQ2(r, "6h"))
                    .addSink(new InfluxDbSink("q2_ranking_6h"))
                    .setParallelism(1)
                    .name("InfluxDB Sink Q2 6h");

            rankedGlobal
                    .map(r -> InfluxDbLineProtocol.fromQ2(r, "global"))
                    .addSink(new InfluxDbSink("q2_ranking_global"))
                    .setParallelism(1)
                    .name("InfluxDB Sink Q2 Global");
        }

        // ── 9. Esecuzione ─────────────────────────────────────────────────────
        env.execute("SABD Project 2 – Query 2");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // ProcessAllWindowFunction per sliding window (1h e 6h)
    // ══════════════════════════════════════════════════════════════════════════

    /**
     * Riceve tutti i voli completati di una finestra sliding, aggrega per
     * aeroporto con una HashMap e produce il top-10.
     *
     * Nota sui duplicati: la sliding window divide internamente le finestre
     * in pane (segmenti non sovrapposti di durata pari allo slide). Flink può
     * invocare process() più volte con gli stessi dati per la stessa finestra
     * logica. La deduplicazione viene gestita nel sink CSV tramite HashSet.
     * Per InfluxDB non serve: stessa combination tag+timestamp → upsert.
     *
     * Il timestamp "ts" nel CSV è context.window().getStart(): per una finestra
     * 6h con slide 30min il primo start può precedere il dataset (es. 2024-12-31)
     * perché Flink allinea le finestre all'epoch. Questo è il comportamento
     * standard di Flink e viene documentato nella relazione.
     */
    public static class Q2SlidingRankingFunction
            extends ProcessAllWindowFunction<FlightEvent, Q2RankedEntry, TimeWindow> {

        @Override
        public void process(Context context,
                            Iterable<FlightEvent> elements,
                            Collector<Q2RankedEntry> out) {

            Map<Integer, Q2AirportStats> airportMap = new HashMap<>();
            long windowStart = context.window().getStart();

            for (FlightEvent e : elements) {
                Q2AirportStats stats = airportMap.computeIfAbsent(
                        e.getOriginAirportId(),
                        id -> { Q2AirportStats s = new Q2AirportStats();
                            s.originAirportId = id; return s; });
                stats.addFlight(e.getAirline(), e.getDestAirportId(), e.getDepDelay());
            }

            emitTopK(airportMap, windowStart, out);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // ProcessAllWindowFunction per finestra global
    // ══════════════════════════════════════════════════════════════════════════

    /**
     * Accumula tutti i voli dall'inizio del dataset (GlobalWindow) e produce
     * il ranking cumulativo ad ogni trigger (ogni 60min di event time).
     *
     * Timestamp di output: inizio fisso del dataset (2025-01-01T00:00:00Z),
     * come da specifica: "timestamp relativo all'inizio della finestra su cui
     * è stata calcolata la classifica". Per la GlobalWindow l'inizio è sempre
     * l'inizio del dataset. Più leggibile di GlobalWindow.getStart() che
     * vale Long.MIN_VALUE.
     */
    public static class Q2GlobalRankingFunction
            extends ProcessAllWindowFunction<FlightEvent, Q2RankedEntry, GlobalWindow> {

        // Epoch ms di 2025-01-01 00:00:00 UTC — inizio fisso del dataset
        private static final long DATASET_START_MS =
                java.time.Instant.parse("2025-01-01T00:00:00Z").toEpochMilli();

        @Override
        public void process(Context context,
                            Iterable<FlightEvent> elements,
                            Collector<Q2RankedEntry> out) {

            Map<Integer, Q2AirportStats> airportMap = new HashMap<>();

            for (FlightEvent e : elements) {
                if (e.getOriginAirportId() == null) continue;
                Q2AirportStats stats = airportMap.computeIfAbsent(
                        e.getOriginAirportId(),
                        id -> { Q2AirportStats s = new Q2AirportStats();
                            s.originAirportId = id; return s; });
                stats.addFlight(e.getAirline(), e.getDestAirportId(), e.getDepDelay());
            }

            emitTopK(airportMap, DATASET_START_MS, out);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Logica di ranking condivisa
    // ══════════════════════════════════════════════════════════════════════════

    private static void emitTopK(Map<Integer, Q2AirportStats> airportMap,
                                 long windowStart,
                                 Collector<Q2RankedEntry> out) {
        List<Q2AirportStats> statsList = new ArrayList<>(airportMap.values());
        statsList.removeIf(s -> s.numFlights < MIN_FLIGHTS || s.severeDelays == 0);
        statsList.sort(Comparator
                .comparingInt((Q2AirportStats s) -> s.severeDelays).reversed()
                .thenComparingInt(s -> s.originAirportId));

        int limit = Math.min(10, statsList.size());
        for (int i = 0; i < limit; i++) {
            Q2AirportStats s = statsList.get(i);
            out.collect(new Q2RankedEntry(
                    windowStart, i + 1, s.originAirportId,
                    s.numFlights, s.severeDelays,
                    s.getDepDelayMean(), s.getDepDelayMaxSafe(),
                    s.getTopDelayedFlights()));
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Formattazione CSV
    // ══════════════════════════════════════════════════════════════════════════

    private static String formatCsvRow(Q2RankedEntry r) {
        StringBuilder sb = new StringBuilder("[");
        if (r.delayedFlights != null && !r.delayedFlights.isEmpty()) {
            for (int i = 0; i < r.delayedFlights.size(); i++) {
                sb.append(r.delayedFlights.get(i).toString());
                if (i < r.delayedFlights.size() - 1) sb.append(", ");
            }
        }
        sb.append("]");

        return String.join(", ",
                CsvOutputFormatter.formatTimestamp(r.windowStart),
                String.valueOf(r.rank),
                String.valueOf(r.originAirportId),
                String.valueOf(r.numFlights),
                String.valueOf(r.severeDelays),
                CsvOutputFormatter.formatDouble(r.depDelayMean),
                CsvOutputFormatter.formatDouble(r.depDelayMax),
                sb.toString());
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Sink CSV con header e deduplicazione — pattern identico a Query1Job
    // ══════════════════════════════════════════════════════════════════════════

    /**
     * Scrive l'header e aggiunge un sink in append con deduplicazione.
     *
     * La deduplicazione tramite HashSet elimina le righe duplicate prodotte
     * dai pane interni della sliding window: due righe identiche (stesso
     * timestamp, stesso rank, stesso aeroporto, stessi valori) vengono
     * scritte una sola volta.
     *
     * Il HashSet cresce con il numero di righe uniche scritte (~57.000 su
     * 4 mesi con slide 30min): memoria accettabile su singolo nodo.
     */
    private static void writeWithHeader(DataStream<String> stream, String outputPath) {
        try (java.io.PrintWriter pw = new java.io.PrintWriter(
                new java.io.FileWriter(outputPath, false))) {
            pw.println(CSV_HEADER);
        } catch (java.io.IOException e) {
            throw new RuntimeException("Impossibile scrivere l'header in " + outputPath, e);
        }

        stream.addSink(new org.apache.flink.streaming.api.functions.sink.RichSinkFunction<String>() {
            private transient java.io.PrintWriter writer;
            private transient Set<String> seen;

            @Override
            public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
                writer = new java.io.PrintWriter(new java.io.FileWriter(outputPath, true));
                seen = new HashSet<>();
            }

            @Override
            public void invoke(String value, Context context) {
                // seen.add() restituisce true solo se la riga non era già presente
                if (seen.add(value)) {
                    writer.println(value);
                    writer.flush();
                }
            }

            @Override
            public void close() {
                if (writer != null) writer.close();
            }
        }).setParallelism(1);
    }
}