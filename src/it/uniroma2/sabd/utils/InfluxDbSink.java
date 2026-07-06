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
 * Sink Flink che scrive su InfluxDB tramite Line Protocol via HTTP.
 *
 * Strategia di scrittura atomica per trigger:
 * - I record con lo stesso timestamp vengono bufferati.
 * - Il flush avviene in due casi:
 *   1. Arriva un record con timestamp DIVERSO dal precedente → flush del buffer precedente.
 *   2. Il buffer raggiunge MAX_BATCH_SIZE (10) → flush immediato dello stesso trigger.
 *      Questo garantisce che l'ultimo trigger venga scritto anche se non arriva
 *      mai un record con timestamp diverso (caso delle sliding windows alla fine).
 * - Il close() esegue un flush finale per sicurezza.
 */
public class InfluxDbSink extends RichSinkFunction<String> {

    private static final long serialVersionUID = 1L;

    // Numero massimo di record per trigger (top-10 di Q2)
    // Quando il buffer raggiunge questa dimensione, flush immediato
    private static final int MAX_BATCH_SIZE = 10;

    private final String measurement;

    private transient String writeUrl;
    private transient String token;

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

        // Caso 1: timestamp diverso → flush del trigger precedente
        if (ts != lastTimestamp && !buffer.isEmpty()) {
            flush();
        }

        buffer.add(lineProtocol);
        lastTimestamp = ts;

        // Caso 2: buffer pieno (10 record = ranking completo) → flush immediato
        // Garantisce la scrittura anche se non arriva mai un record successivo
        if (buffer.size() >= MAX_BATCH_SIZE) {
            flush();
        }
    }

    @Override
    public void close() throws Exception {
        // Flush finale per eventuali record residui (finestre con < 10 aeroporti)
        if (!buffer.isEmpty()) {
            flush();
        }
    }

    private void flush() throws IOException {
        if (buffer.isEmpty()) return;

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