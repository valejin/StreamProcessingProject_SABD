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
 * ── Gestione delle ore notturne (ore senza voli) ────────────────────────────
 *
 * Problema: nelle ore notturne (tipicamente 00:00–05:00) il Producer non invia
 * voli reali. Il watermark Flink smette di avanzare e il ContinuousEventTimeTrigger
 * non scatta, producendo buchi nel CSV.
 *
 * Soluzione adottata – Heartbeat dal Producer:
 *   Il Producer inserisce messaggi heartbeat fittizi (flag "heartbeat": true)
 *   a intervalli di HEARTBEAT_INTERVAL_MS (default 10 min di Event Time)
 *   ogni volta che il gap tra due eventi reali consecutivi supera
 *   HEARTBEAT_THRESHOLD_MS (default 5 min di Event Time).
 *
 *   Il WatermarkStrategy elabora gli heartbeat normalmente: il loro eventTime
 *   è un timestamp valido nel gap notturno, quindi fa avanzare il watermark
 *   e sblocca il trigger.
 *
 *   Gli heartbeat vengono filtrati in Flink con e.isHeartbeat() PRIMA di
 *   qualsiasi aggregazione: non inquinano né Q1 né Q2.
 *
 * Vantaggi rispetto alle alternative:
 *   - Più affidabile di un trigger Processing-Time: il timing del Producer
 *     è sincronizzato con il TIME_SCALE_FACTOR, mentre un timer wall-clock
 *     in Flink si desincronizza durante checkpoint o pause del job.
 *   - Più semplice di un Custom Trigger Event+ProcessingTime.
 *   - Retrocompatibile: messaggi senza il campo "heartbeat" vengono trattati
 *     come heartbeat=false (nessuna modifica al comportamento esistente).
 *
 * ── Struttura della pipeline ─────────────────────────────────────────────────
 *
 * Kafka → WatermarkStrategy → filtro heartbeat+completati → tre windowAll:
 *   1h sliding → Q2SlidingRankingFunction → CSV + InfluxDB
 *   6h sliding → Q2SlidingRankingFunction → CSV + InfluxDB
 *   global     → Q2GlobalRankingFunction  → CSV + InfluxDB
 *
 * La deduplicazione dei pane duplicati della sliding window avviene nel sink
 * CSV tramite HashSet. Il sink InfluxDB non necessita di deduplicazione:
 * stessa combinazione tag+timestamp → upsert nativo.
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

    private static final boolean INFLUXDB_ENABLED =
            Boolean.parseBoolean(System.getenv().getOrDefault("INFLUXDB_ENABLED", "true"));

    private static final int MIN_FLIGHTS = 30;

    // Header CSV per le sliding window: ts = inizio della finestra temporale
    private static final String CSV_HEADER =
            "ts, rank, origin_airport_id, num_flights, severe_delays, " +
                    "dep_delay_mean, dep_delay_max, delayed_flights";

    // Header CSV per la global window: ts = inizio della finestra globale,
    // fisso a 2025-01-01 00:00:00 per tutti i trigger (la GlobalWindow non si
    // chiude mai e inizia sempre dall'inizio del dataset).
    // La deduplicazione HashSet è disabilitata per questa finestra (sink separato
    // senza HashSet), quindi ts costante non causa perdita di righe.
    private static final String CSV_HEADER_GLOBAL =
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

        // ── 3. Pre-filtro: scarta heartbeat e voli non completati ─────────────
        //
        // IMPORTANTE: il filtro degli heartbeat deve avvenire DOPO l'assegnazione
        // del watermark (fromSource) e PRIMA delle finestre. In questo modo:
        //   - Il WatermarkStrategy vede gli heartbeat → il watermark avanza.
        //   - Le finestre ricevono solo voli reali → le statistiche sono corrette.
        //
        // NON filtrare gli heartbeat direttamente nella sorgente o nel
        // WatermarkStrategy: si perderebbe l'avanzamento del watermark.
        DataStream<FlightEvent> completedFlights = flights
                .filter(e -> !e.isHeartbeat() && e.isCompleted() && e.getOriginAirportId() != null);

        // ── 4. Finestra 1h sliding (slide 60min) ─────────────────────────────
        DataStream<Q2RankedEntry> ranked1h = completedFlights
                .windowAll(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(60)))
                .process(new Q2SlidingRankingFunction());

        // ── 5. Finestra 6h sliding (slide 60min) ─────────────────────────────
        DataStream<Q2RankedEntry> ranked6h = completedFlights
                .windowAll(SlidingEventTimeWindows.of(Time.hours(6), Time.minutes(60)))
                .process(new Q2SlidingRankingFunction());

        // ── 6. Finestra global (trigger ogni 60min di event time) ─────────────
        //
        // Con gli heartbeat, il watermark avanza regolarmente anche di notte:
        // il ContinuousEventTimeTrigger scatta ogni 60 min di Event Time
        // senza buchi, indipendentemente dalla presenza di voli reali.
        DataStream<Q2RankedEntry> rankedGlobal = completedFlights
                .windowAll(GlobalWindows.create())
                .trigger(ContinuousEventTimeTrigger.of(Time.minutes(60)))
                .process(new Q2GlobalRankingFunction());

        // ── 7. Sink CSV con header e deduplicazione ───────────────────────────
        //
        // La global window usa formatCsvRowGlobal + CSV_HEADER_GLOBAL:
        //   - ts = DATASET_START_MS (2025-01-01 00:00:00): inizio semantico della
        //     GlobalWindow, costante per tutti i trigger
        //   - influxTs = ora virtuale del trigger (07:00, 08:00, ...): distinto per
        //     ogni scatto, usato da InfluxDB. Sink senza HashSet → nessuna perdita di righe
        // Sliding window: deduplicazione HashSet necessaria per i pane duplicati
        writeWithHeader(ranked1h.map(Query2Job::formatCsvRow),   OUTPUT_PATH_1H);
        writeWithHeader(ranked6h.map(Query2Job::formatCsvRow),   OUTPUT_PATH_6H);
        // Global window: NO deduplicazione — ogni trigger emette sempre righe nuove
        // (le statistiche cumulative cambiano ad ogni trigger), e ts=DATASET_START
        // è costante per semantica corretta (la finestra inizia dall'inizio del dataset).
        writeWithoutDedup(rankedGlobal.map(Query2Job::formatCsvRowGlobal), OUTPUT_PATH_GLOBAL, CSV_HEADER_GLOBAL);

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
     * Riceve tutti i voli completati (heartbeat già filtrati) di una finestra
     * sliding, aggrega per aeroporto con una HashMap e produce il top-10.
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
                // Doppio controllo difensivo: gli heartbeat non dovrebbero arrivare
                // qui (già filtrati nel pre-filtro), ma se lo facessero avrebbero
                // originAirportId=null e verrebbero ignorati dal computeIfAbsent.
                if (e.isHeartbeat() || e.getOriginAirportId() == null) continue;

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
     * Con gli heartbeat, il trigger scatta regolarmente anche di notte:
     * il ranking globale viene aggiornato ogni ora virtuale senza buchi,
     * riflettendo l'accumulo progressivo di tutti i voli fino a quell'istante.
     *
     * Timestamp di output: inizio fisso del dataset (2025-01-01T00:00:00Z).
     * Il timestamp registrato è l'inizio dell'ora virtuale del trigger,
     * calcolato come floor(maxEventTime / 60min) * 60min — coerente con
     * il ts della finestra 1h e corretto per la visualizzazione in Grafana.
     */
    public static class Q2GlobalRankingFunction
            extends ProcessAllWindowFunction<FlightEvent, Q2RankedEntry, GlobalWindow> {

        // Epoch ms di 2025-01-01 00:00:00 UTC — inizio fisso della GlobalWindow
        private static final long DATASET_START_MS =
                java.time.Instant.parse("2025-01-01T00:00:00Z").toEpochMilli();

        @Override
        public void process(Context context,
                            Iterable<FlightEvent> elements,
                            Collector<Q2RankedEntry> out) {

            Map<Integer, Q2AirportStats> airportMap = new HashMap<>();
            long maxEventTime = Long.MIN_VALUE;

            for (FlightEvent e : elements) {
                // Gli heartbeat non arrivano qui (filtrati nel pre-filtro),
                // ma il controllo difensivo garantisce correttezza anche in
                // configurazioni inusuali (es. filtro rimosso per debug).
                if (e.isHeartbeat() || e.getOriginAirportId() == null) continue;

                if (e.getEventTime() > maxEventTime) {
                    maxEventTime = e.getEventTime();
                }

                Q2AirportStats stats = airportMap.computeIfAbsent(
                        e.getOriginAirportId(),
                        id -> { Q2AirportStats s = new Q2AirportStats();
                            s.originAirportId = id; return s; });
                stats.addFlight(e.getAirline(), e.getDestAirportId(), e.getDepDelay());
            }

            // influxTs: usato da InfluxDB per distinguere snapshot consecutivi.
            // Ricavato come floor(maxEventTime / 60min) * 60min — corrisponde
            // all'inizio dell'ora virtuale del trigger (07:00, 08:00, ...) ed è
            // strettamente crescente, necessario per la corretta visualizzazione
            // in Grafana.
            // ProcessAllWindowFunction.Context non espone currentWatermark(),
            // quindi tracciamo il massimo event time manualmente.
            final long INTERVAL_MS = 60 * 60 * 1000L;
            long influxTs = (maxEventTime != Long.MIN_VALUE)
                    ? (maxEventTime / INTERVAL_MS) * INTERVAL_MS
                    : DATASET_START_MS;

            List<Q2AirportStats> statsList = new ArrayList<>(airportMap.values());
            statsList.removeIf(s -> s.numFlights < MIN_FLIGHTS || s.severeDelays == 0);
            statsList.sort(Comparator
                    .comparingInt((Q2AirportStats s) -> s.severeDelays).reversed()
                    .thenComparingInt(s -> s.originAirportId));

            int limit = Math.min(10, statsList.size());
            for (int i = 0; i < limit; i++) {
                Q2AirportStats s = statsList.get(i);
                // windowStart = DATASET_START_MS: ts nel CSV è sempre l'inizio
                // della GlobalWindow (2025-01-01 00:00:00), semanticamente corretto.
                // influxTs = ora virtuale del trigger: distinto per ogni scatto,
                // evita collisioni in InfluxDB.
                Q2RankedEntry entry = new Q2RankedEntry(
                        DATASET_START_MS, i + 1, s.originAirportId,
                        s.numFlights, s.severeDelays,
                        s.getDepDelayMean(), s.getDepDelayMaxSafe(),
                        s.getTopDelayedFlights());
                entry.influxTs = influxTs;
                out.collect(entry);
            }
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

    /**
     * Formatta una riga CSV per la global window.
     *
     * Usa {@code windowStart} come timestamp, che per la global window è sempre
     * {@code DATASET_START_MS} (2025-01-01 00:00:00): l'inizio semanticamente
     * corretto della GlobalWindow, che non si chiude mai.
     * La deduplicazione HashSet è disabilitata per questo sink (writeWithoutDedup),
     * quindi ts costante non causa perdita di righe tra trigger consecutivi.
     */
    private static String formatCsvRowGlobal(Q2RankedEntry r) {
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
    // Sink CSV con header e deduplicazione
    // ══════════════════════════════════════════════════════════════════════════

    /**
     * Scrive l'header e aggiunge un sink in append con deduplicazione.
     * Usa CSV_HEADER come header predefinito (per sliding window).
     */
    private static void writeWithHeader(DataStream<String> stream, String outputPath) {
        writeWithHeader(stream, outputPath, CSV_HEADER);
    }

    /**
     * Scrive l'header specificato e aggiunge un sink in append con deduplicazione.
     * Usato per le sliding window: l'HashSet elimina le righe duplicate prodotte
     * dai pane interni di Flink.
     */
    private static void writeWithHeader(DataStream<String> stream, String outputPath,
                                        String header) {
        try (java.io.PrintWriter pw = new java.io.PrintWriter(
                new java.io.FileWriter(outputPath, false))) {
            pw.println(header);
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

    /**
     * Scrive l'header e aggiunge un sink in append SENZA deduplicazione.
     * Usato per la global window: ogni trigger produce righe con ts costante
     * (DATASET_START_MS) ma statistiche sempre diverse (cumulative crescenti),
     * quindi non ci sono duplicati reali da eliminare. L'HashSet causerebbe
     * invece la perdita di righe legittime quando le statistiche di un aeroporto
     * non cambiano tra due trigger consecutivi.
     */
    private static void writeWithoutDedup(DataStream<String> stream, String outputPath,
                                          String header) {
        try (java.io.PrintWriter pw = new java.io.PrintWriter(
                new java.io.FileWriter(outputPath, false))) {
            pw.println(header);
        } catch (java.io.IOException e) {
            throw new RuntimeException("Impossibile scrivere l'header in " + outputPath, e);
        }

        stream.addSink(new org.apache.flink.streaming.api.functions.sink.RichSinkFunction<String>() {
            private transient java.io.PrintWriter writer;

            @Override
            public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
                writer = new java.io.PrintWriter(new java.io.FileWriter(outputPath, true));
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
    }
}