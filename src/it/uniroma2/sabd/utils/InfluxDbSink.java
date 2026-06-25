package it.uniroma2.sabd.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * Sink Flink che scrive su InfluxDB 2.x tramite Line Protocol via HTTP.
 *
 * Utilizza java.net.HttpURLConnection (disponibile nel JDK 11 senza
 * dipendenze aggiuntive) per mantenere il JAR leggero.
 *
 * Il Line Protocol di InfluxDB ha la forma:
 *   measurement[,tag_key=tag_value ...] field_key=field_value[,...] [timestamp]
 * dove il timestamp è in nanosecondi (default).
 *
 * Uso tipico:
 *   stream.addSink(new InfluxDbSink("q1_metrics"))
 *         .setParallelism(1);
 *
 * Configurazione tramite variabili d'ambiente (con default per sviluppo):
 *   INFLUXDB_URL    → http://influxdb:8086
 *   INFLUXDB_TOKEN  → sabd-influx-token-2025
 *   INFLUXDB_ORG    → sabd
 *   INFLUXDB_BUCKET → flights
 */
public class InfluxDbSink extends RichSinkFunction<String> {

    private static final long serialVersionUID = 1L;

    private final String measurement;

    // Configurazione (letta all'apertura del sink, non nel costruttore,
    // per evitare problemi di serializzazione in ambienti distribuiti)
    private transient String writeUrl;
    private transient String token;

    /**
     * @param measurement nome della measurement InfluxDB (es. "q1_metrics", "q2_ranking")
     */
    public InfluxDbSink(String measurement) {
        this.measurement = measurement;
    }

    @Override
    public void open(Configuration parameters) {
        String influxUrl = System.getenv().getOrDefault("INFLUXDB_URL",   "http://influxdb:8086");
        String org       = System.getenv().getOrDefault("INFLUXDB_ORG",   "sabd");
        String bucket    = System.getenv().getOrDefault("INFLUXDB_BUCKET","flights");
        token            = System.getenv().getOrDefault("INFLUXDB_TOKEN", "sabd-influx-token-2025");

        writeUrl = influxUrl + "/api/v2/write"
                + "?org="       + org
                + "&bucket="    + bucket
                + "&precision=" + "ms";  // timestamp in millisecondi
    }

    /**
     * Scrive una riga in Line Protocol su InfluxDB.
     *
     * Il valore {@code lineProtocol} deve già essere formattato correttamente
     * (vedi InfluxDbLineProtocol per i builder Q1/Q2).
     */
    @Override
    public void invoke(String lineProtocol, Context context) throws IOException {
        if (lineProtocol == null || lineProtocol.isBlank()) return;

        byte[] body = lineProtocol.getBytes(StandardCharsets.UTF_8);

        HttpURLConnection conn = (HttpURLConnection) new URL(writeUrl).openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Authorization", "Token " + token);
        conn.setRequestProperty("Content-Type",  "text/plain; charset=utf-8");
        conn.setDoOutput(true);
        conn.setConnectTimeout(5_000);
        conn.setReadTimeout(5_000);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(body);
        }

        int code = conn.getResponseCode();
        if (code != 204) {
            // 204 No Content = scrittura OK per InfluxDB 2.x
            throw new IOException("InfluxDB write failed [" + code + "] for: " + lineProtocol);
        }
    }
}
