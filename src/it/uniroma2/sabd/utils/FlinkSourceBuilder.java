package it.uniroma2.sabd.utils;

import it.uniroma2.sabd.model.FlightEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.time.Duration;

/**
 * Factory centralizzata per la creazione della sorgente Kafka e della
 * WatermarkStrategy.  Centralizzare qui evita di duplicare la configurazione
 * tra Q1 e Q2.
 *
 * ── Scelta della WatermarkStrategy ─────────────────────────────────────────
 *
 * Il dataset è pre-ordinato per event time dal Producer prima dell'invio a Kafka.
 * Il disordine residuo è causato da due fenomeni:
 *
 *  1. Partizioni Kafka: se il topic ha N > 1 partizioni, Flink legge le
 *     partizioni in parallelo e l'ordinamento globale non è garantito.
 *     Due messaggi con event time t1 < t2 possono arrivare invertiti
 *     se appartengono a partizioni diverse.
 *
 *  2. Latenze variabili di rete e batching: anche con una sola partizione,
 *     il meccanismo di batching del consumer Kafka può alterare l'ordine
 *     su scala di pochi millisecondi.
 *
 * BoundedOutOfOrderness con maxOutOfOrderness = 5 minuti di event time.
 *
 * Motivazione:
 *  - Con TIME_SCALE_FACTOR = 86400, 5 minuti di event time corrisponde a una
 *    latenza aggiuntiva trascurabile.
 *  - È sufficiente ad assorbire il disordine da parallelismo Kafka su una
 *    singola macchina di sviluppo.
 *
 *
 * Effetto sulle finestre:
 *  Una finestra [08:00, 09:00) viene chiusa quando il watermark supera
 *  09:00:00, ossia quando Flink osserva un evento con event time ≥ 09:05:00.
 *  Tutti gli eventi con event time nella finestra che arrivano prima di quel
 *  momento vengono inclusi nel calcolo; quelli che arrivano dopo vengono scartati
 *  come "late events" (comportamento di default: drop senza side output).
 */
public class FlinkSourceBuilder {

    /**
     * Ritardo massimo ammesso per gli eventi fuori ordine.
     * Espresso come Duration di event time (non wall-clock).
     */
    public static final Duration MAX_OUT_OF_ORDERNESS = Duration.ofMinutes(5);

    /**
     * Crea il KafkaSource configurato per leggere il topic dei voli.
     *
     * @param bootstrapServers  indirizzo del broker Kafka (es. "kafka:29092")
     * @param topic             nome del topic (es. "flights")
     * @param groupId           consumer group ID (usare valori distinti per Q1/Q2
     *                          se si vuole che leggano indipendentemente)
     * @return KafkaSource<FlightEvent> pronto per DataStreamSource
     */
    public static KafkaSource<FlightEvent> buildKafkaSource(
            String bootstrapServers,
            String topic,
            String groupId) {

        return KafkaSource.<FlightEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new FlightEventDeserializer())
                .build();
    }

    /**
     * Restituisce la WatermarkStrategy adottata per il progetto.
     *
     * BoundedOutOfOrderness:
     *  - Emette watermark = max_event_time_seen - MAX_OUT_OF_ORDERNESS
     *  - Il watermark avanza monotonicamente con il massimo event time osservato
     *  - Compatibile con l'ordinamento quasi-perfetto garantito dal Producer
     *
     * withTimestampAssigner: estrae l'epoch ms dall'event time del FlightEvent
     * (già calcolato dal Producer e incluso nel messaggio JSON).
     */
    public static WatermarkStrategy<FlightEvent> buildWatermarkStrategy() {
        return WatermarkStrategy
                .<FlightEvent>forBoundedOutOfOrderness(MAX_OUT_OF_ORDERNESS)
                .withTimestampAssigner(
                        (event, recordTimestamp) -> event.getEventTime()
                );
    }
}