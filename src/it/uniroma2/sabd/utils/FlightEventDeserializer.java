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
 * Formato atteso (esempio):
 * {
 *   "event_time":        "2025-01-01T08:35:00",
 *   "airline":           "AA",
 *   "origin_airport_id": 12478,
 *   "dest_airport_id":   11298,
 *   "crs_dep_time":      835,
 *   "dep_delay":         11.0,
 *   "cancelled":         0,
 *   "diverted":          0
 * }
 *
 * Gestione dei valori null:
 *  - dep_delay null  → FlightEvent.depDelay = null
 *    (i voli cancellati spesso non hanno dep_delay; Flink li filtrerà dove necessario)
 *  - cancelled null  → trattato come 0 (non cancellato) per scelta conservativa
 *  - diverted null   → trattato come 0 (non deviato)
 *  - airline null    → FlightEvent.airline = null (sarà filtrato nelle query)
 *
 * L'event_time è in formato ISO 8601 locale (senza timezone): viene interpretato
 * come UTC, coerentemente con come il Producer ha costruito i timestamp.
 */
public class FlightEventDeserializer implements DeserializationSchema<FlightEvent> {

    private static final long serialVersionUID = 1L;

    /** Pattern ISO 8601 locale usato dal producer Python (strftime "%Y-%m-%dT%H:%M:%S"). */
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    /**
     * ObjectMapper è thread-safe per le operazioni di lettura dopo la configurazione,
     * ma DeserializationSchema può essere condiviso tra thread: dichiariamo transient
     * e ricreiamo lazily per sicurezza.
     */
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
        // Converte la stringa ISO in epoch ms (UTC).
        // Il Producer ha costruito l'event_time come orario locale del volo
        // senza informazioni di timezone: lo trattiamo come UTC.
        long eventTimeMs = 0L;
        JsonNode etNode = node.get("event_time");
        if (etNode != null && !etNode.isNull()) {
            LocalDateTime ldt = LocalDateTime.parse(etNode.asText(), FORMATTER);
            eventTimeMs = ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
        }

        // ── Campi stringa ─────────────────────────────────────────────────────
        String airline = getTextOrNull(node, "airline");

        // ── Campi interi (nullable) ───────────────────────────────────────────
        Integer originAirportId = getIntOrNull(node, "origin_airport_id");
        Integer destAirportId   = getIntOrNull(node, "dest_airport_id");
        Integer crsDepTime      = getIntOrNull(node, "crs_dep_time");

        // cancelled e diverted: null → 0 (scelta conservativa documentata nel report)
        Integer cancelled = getIntOrDefault(node, "cancelled", 0);
        Integer diverted  = getIntOrDefault(node, "diverted",  0);

        // ── Campi double (nullable) ───────────────────────────────────────────
        // dep_delay null è frequente per voli cancellati; lo manteniamo null
        // per distinguerlo da "ritardo zero" nelle aggregazioni.
        Double depDelay = getDoubleOrNull(node, "dep_delay");

        return new FlightEvent(
                eventTimeMs, airline,
                originAirportId, destAirportId,
                crsDepTime, depDelay,
                cancelled, diverted
        );
    }

    /**
     * Flink chiama isEndOfStream() per sapere se lo stream è terminato.
     * Per stream Kafka continui, restituisce sempre false.
     */
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