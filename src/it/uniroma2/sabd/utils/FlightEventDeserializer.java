package it.uniroma2.sabd.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.sabd.model.FlightEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Deserializzatore per i messaggi JSON inviati dal producer Kafka.
 *
 * Formato atteso per un volo reale:
 * {
 *   "event_time":          "2025-01-01T08:35:00",
 *   "kafka_produce_time":  1751000000123,
 *   "airline":             "AA",
 *   "origin_airport_id":   12478,
 *   "dest_airport_id":     11298,
 *   "crs_dep_time":        835,
 *   "dep_delay":           11.0,
 *   "cancelled":           0,
 *   "diverted":            0
 * }
 *
 * Formato atteso per un heartbeat (tick fittizio notturno):
 * {
 *   "event_time":          "2025-01-15T02:30:00",
 *   "kafka_produce_time":  1751000060456,
 *   "heartbeat":           true,
 *   ...
 * }
 *
 * I messaggi heartbeat vengono deserializzati normalmente: il WatermarkStrategy
 * legge il loro eventTime e fa avanzare il watermark. Il campo isHeartbeat()
 * viene poi usato dai filtri in Query1Job e Query2Job per escluderli dalle
 * aggregazioni prima che entrino nelle finestre.
 *
 * Retrocompatibilità: i messaggi senza il campo "heartbeat" (es. prodotti da
 * versioni precedenti del producer) vengono trattati come heartbeat=false.
 */
public class FlightEventDeserializer implements DeserializationSchema<FlightEvent> {

    private static final long serialVersionUID = 1L;

    /** Pattern ISO 8601 locale usato dal producer Python (strftime "%Y-%m-%dT%H:%M:%S"). */
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    private transient ObjectMapper mapper;

    private ObjectMapper getMapper() {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        return mapper;
    }

    @Override
    public FlightEvent deserialize(byte[] message) throws IOException {
        JsonNode node = getMapper().readTree(message);

        // ── Event time ────────────────────────────────────────────────────────
        long eventTimeMs = 0L;
        JsonNode etNode = node.get("event_time");
        if (etNode != null && !etNode.isNull()) {
            LocalDateTime ldt = LocalDateTime.parse(etNode.asText(), FORMATTER);
            eventTimeMs = ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
        }

        // ── Flag heartbeat ────────────────────────────────────────────────────
        // Retrocompatibile: assente → false (volo reale).
        JsonNode hbNode = node.get("heartbeat");
        boolean isHeartbeat = (hbNode != null && !hbNode.isNull() && hbNode.asBoolean(false));

        // ── Kafka produce time (wall-clock ms del producer) ───────────────────
        // Retrocompatibile: assente o null → 0L (latenza non calcolabile).
        JsonNode kptNode = node.get("kafka_produce_time");
        long kafkaProduceTime = (kptNode != null && !kptNode.isNull()) ? kptNode.asLong(0L) : 0L;

        // ── Campi volo (null per gli heartbeat per scelta semantica) ──────────
        String  airline         = getTextOrNull(node, "airline");
        Integer originAirportId = getIntOrNull(node, "origin_airport_id");
        Integer destAirportId   = getIntOrNull(node, "dest_airport_id");
        Integer crsDepTime      = getIntOrNull(node, "crs_dep_time");
        Integer cancelled       = getIntOrDefault(node, "cancelled", 0);
        Integer diverted        = getIntOrDefault(node, "diverted",  0);
        Double  depDelay        = getDoubleOrNull(node, "dep_delay");

        FlightEvent event = new FlightEvent(
                eventTimeMs, airline,
                originAirportId, destAirportId,
                crsDepTime, depDelay,
                cancelled, diverted
        );
        event.setHeartbeat(isHeartbeat);
        event.setKafkaProduceTime(kafkaProduceTime);
        return event;
    }

    @Override
    public boolean isEndOfStream(FlightEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<FlightEvent> getProducedType() {
        return TypeInformation.of(FlightEvent.class);
    }

    // ─── Metodi di utilità per la lettura sicura dei nodi JSON ────────────────

    private String getTextOrNull(JsonNode node, String field) {
        JsonNode n = node.get(field);
        return (n == null || n.isNull()) ? null : n.asText();
    }

    private Integer getIntOrNull(JsonNode node, String field) {
        JsonNode n = node.get(field);
        return (n == null || n.isNull()) ? null : n.asInt();
    }

    private Integer getIntOrDefault(JsonNode node, String field, int defaultValue) {
        JsonNode n = node.get(field);
        return (n == null || n.isNull()) ? defaultValue : n.asInt();
    }

    private Double getDoubleOrNull(JsonNode node, String field) {
        JsonNode n = node.get(field);
        return (n == null || n.isNull()) ? null : n.asDouble();
    }
}