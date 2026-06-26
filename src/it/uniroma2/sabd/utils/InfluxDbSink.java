package it.uniroma2.sabd.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Sink Flink che scrive su InfluxDB 2.x tramite Line Protocol via HTTP.
 *
 * Per la global window (e in generale per qualsiasi trigger che emette
 * più record con lo stesso timestamp), i record vengono bufferati e
 * scritti in una singola chiamata HTTP atomica.
 *
 * La logica di flush è:
 *   - se il timestamp del record corrente è DIVERSO dall'ultimo visto
 *     → flush del buffer precedente, poi aggiungi il nuovo record
 *   - se il timestamp è UGUALE all'ultimo visto
 *     → aggiungi al buffer (stesso trigger)
 *   - al close() → flush finale del buffer residuo
 *
 * Questo garantisce che tutti i record dello stesso trigger vengano
 * scritti atomicamente in InfluxDB, eliminando il problema di Grafana
 * che vede ranking parziali durante il refresh.
 */
public class InfluxDbSink extends RichSinkFunction<String> {

    private static final long serialVersionUID = 1L;

    private final String measurement;

    private transient String writeUrl;
    private transient String token;

    // Buffer per scrittura atomica per trigger
    private transient List<String> buffer;
    private transient long lastTimestamp;

    public InfluxDbSink(String measurement) {
        this.measurement = measurement;
    }

    @Override
    public void open(Configuration parameters) {
        String influxUrl = System.getenv().getOrDefault("INFLUXDB_URL",    "http://influxdb:8086");
        String org       = System.getenv().getOrDefault("INFLUXDB_ORG",    "sabd");
        String bucket    = System.getenv().getOrDefault("INFLUXDB_BUCKET", "flights");
        token            = System.getenv().getOrDefault("INFLUXDB_TOKEN",  "sabd-influx-token-2025");

        writeUrl = influxUrl + "/api/v2/write"
                + "?org="       + org
                + "&bucket="    + bucket
                + "&precision=" + "ms";

        buffer        = new ArrayList<>();
        lastTimestamp = Long.MIN_VALUE;
    }

    /**
     * Estrae il timestamp (ultimo token) dalla riga Line Protocol.
     * Formato: "measurement,tags fields timestamp"
     */
    private static long extractTimestamp(String lineProtocol) {
        int lastSpace = lineProtocol.lastIndexOf(' ');
        if (lastSpace < 0) return Long.MIN_VALUE;
        try {
            return Long.parseLong(lineProtocol.substring(lastSpace + 1).trim());
        } catch (NumberFormatException e) {
            return Long.MIN_VALUE;
        }
    }

    @Override
    public void invoke(String lineProtocol, Context context) throws IOException {
        if (lineProtocol == null || lineProtocol.isBlank()) return;

        long ts = extractTimestamp(lineProtocol);

        if (ts != lastTimestamp && !buffer.isEmpty()) {
            // Timestamp diverso → flush atomico del trigger precedente
            flush();
        }

        buffer.add(lineProtocol);
        lastTimestamp = ts;
    }

    /**
     * Flush finale: scrive i record residui nel buffer.
     * Chiamato da Flink quando il sink viene chiuso.
     */
    @Override
    public void close() throws Exception {
        if (!buffer.isEmpty()) {
            flush();
        }
    }

    /**
     * Scrive tutti i record del buffer in una singola chiamata HTTP.
     * InfluxDB accetta più righe Line Protocol separate da '\n'.
     */
    private void flush() throws IOException {
        if (buffer.isEmpty()) return;

        // Unisci tutte le righe con newline → una sola chiamata HTTP
        String body = String.join("\n", buffer);
        buffer.clear();

        writeToInflux(body);
    }

    private void writeToInflux(String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);

        HttpURLConnection conn = (HttpURLConnection) new URL(writeUrl).openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Authorization", "Token " + token);
        conn.setRequestProperty("Content-Type",  "text/plain; charset=utf-8");
        conn.setDoOutput(true);
        conn.setConnectTimeout(5_000);
        conn.setReadTimeout(5_000);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(bytes);
        }

        int code = conn.getResponseCode();
        if (code != 204) {
            throw new IOException("InfluxDB write failed [" + code + "] lines:\n" + body);
        }
    }
}