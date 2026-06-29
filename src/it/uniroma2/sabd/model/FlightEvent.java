package it.uniroma2.sabd.model;

import java.io.Serializable;

/**
 * POJO che rappresenta un singolo evento di volo ricevuto da Kafka.
 *
 * Campi selezionati dal Producer in base alle query Q1 e Q2:
 *  - eventTime         : epoch ms calcolato dal Producer (YEAR+MONTH+DAY+CRS_DEP_TIME)
 *  - airline           : codice IATA della compagnia (OP_UNIQUE_CARRIER)
 *  - originAirportId   : aeroporto di partenza (ORIGIN_AIRPORT_ID)
 *  - destAirportId     : aeroporto di destinazione (DEST_AIRPORT_ID)
 *  - crsDepTime        : orario schedulato di partenza in formato HHMM (CRS_DEP_TIME)
 *  - depDelay          : ritardo in partenza in minuti (DEP_DELAY), può essere null
 *  - cancelled         : 1.0 se cancellato, 0.0 altrimenti (CANCELLED)
 *  - diverted          : 1.0 se deviato, 0.0 altrimenti (DIVERTED)
 *  - heartbeat         : true se il messaggio è un tick fittizio inviato dal Producer
 *                        per mantenere il Watermark attivo nelle ore notturne senza voli.
 *                        I messaggi heartbeat hanno tutti gli altri campi null
 *                        e devono essere filtrati prima di qualsiasi aggregazione.
 *
 * Flink richiede che i POJO abbiano:
 *  - costruttore no-arg pubblico
 *  - getter e setter pubblici per ogni campo
 */
public class FlightEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Epoch ms (UTC) corrispondente a YEAR-MONTH-DAY CRS_DEP_TIME. */
    private long eventTime;

    /** Codice IATA della compagnia aerea, es. "AA", "DL", "UA", "WN". Può essere null. */
    private String airline;

    /** ID numerico dell'aeroporto di partenza (ORIGIN_AIRPORT_ID). Può essere null. */
    private Integer originAirportId;

    /** ID numerico dell'aeroporto di destinazione (DEST_AIRPORT_ID). Può essere null. */
    private Integer destAirportId;

    /**
     * Orario schedulato di partenza in formato HHMM (es. 835 → 08:35).
     */
    private Integer crsDepTime;

    /**
     * Ritardo in partenza in minuti (DEP_DELAY).
     * Può essere null per voli cancellati o con dato mancante.
     * Valori negativi indicano partenze in anticipo.
     */
    private Double depDelay;

    /**
     * Flag di cancellazione: 1 = cancellato, 0 = non cancellato.
     */
    private Integer cancelled;

    /** Flag di deviazione: 1 = deviato, 0 = non deviato. */
    private Integer diverted;

    /**
     * Flag heartbeat: true se il messaggio è un tick fittizio inserito dal Producer
     * durante i gap notturni privi di voli, per mantenere il Watermark Flink attivo.
     *
     * Un evento heartbeat NON rappresenta un volo reale: tutti gli altri campi
     * (airline, originAirportId, depDelay, ecc.) sono null.
     *
     * Il filtro in Query1Job e Query2Job deve scartare questi eventi PRIMA
     * di qualsiasi aggregazione tramite il metodo isHeartbeat().
     *
     * Il WatermarkStrategy lo elabora normalmente: il suo eventTime è un
     * timestamp valido nel futuro del gap, quindi fa avanzare il watermark.
     */
    private boolean heartbeat = false;

    /**
     * Wall-clock (epoch ms, UTC) del momento in cui il Producer ha inviato
     * questo messaggio a Kafka. Corrisponde a time.time_ns() // 1_000_000
     * nel producer Python, misurato immediatamente prima della chiamata a
     * producer.produce().
     *
     * Usato per calcolare la latenza end-to-end:
     *   latency_ms = System.currentTimeMillis() [al momento dell'output Flink]
     *                - max(kafkaProduceTime)     [tra tutti gli eventi della finestra]
     *
     * Il valore 0 indica che il campo non è presente nel messaggio (retrocompatibilità
     * con messaggi prodotti prima dell'aggiunta di questa metrica): in tal caso
     * la latenza non viene calcolata per quella finestra.
     */
    private long kafkaProduceTime = 0L;

    // ─── Costruttori ──────────────────────────────────────────────────────────

    /** Costruttore no-arg richiesto da Flink per i POJO. */
    public FlightEvent() {}

    public FlightEvent(long eventTime, String airline,
                       Integer originAirportId, Integer destAirportId,
                       Integer crsDepTime, Double depDelay,
                       Integer cancelled, Integer diverted) {
        this.eventTime       = eventTime;
        this.airline         = airline;
        this.originAirportId = originAirportId;
        this.destAirportId   = destAirportId;
        this.crsDepTime      = crsDepTime;
        this.depDelay        = depDelay;
        this.cancelled       = cancelled;
        this.diverted        = diverted;
        this.heartbeat       = false;
    }

    // ─── Metodi di convenienza (semantica di dominio) ─────────────────────────

    /**
     * Restituisce true se questo messaggio è un heartbeat fittizio.
     * Da usare come primo filtro in ogni pipeline di query:
     *   .filter(e -> !e.isHeartbeat())
     */
    public boolean isHeartbeat() {
        return heartbeat;
    }

    public boolean isCancelled() {
        return cancelled != null && cancelled == 1;
    }

    public boolean isDiverted() {
        return diverted != null && diverted == 1;
    }

    public boolean isCompleted() {
        return !isCancelled() && !isDiverted();
    }

    public boolean hasSevereDelay() {
        return isCompleted() && depDelay != null && depDelay > 30.0;
    }

    public boolean isLate() {
        return !isCancelled() && depDelay != null && depDelay > 15.0;
    }

    // ─── Getter e Setter ──────────────────────────────────────────────────────

    public long getEventTime()                  { return eventTime; }
    public void setEventTime(long eventTime)    { this.eventTime = eventTime; }

    public String getAirline()                  { return airline; }
    public void setAirline(String airline)      { this.airline = airline; }

    public Integer getOriginAirportId()                         { return originAirportId; }
    public void setOriginAirportId(Integer originAirportId)     { this.originAirportId = originAirportId; }

    public Integer getDestAirportId()                           { return destAirportId; }
    public void setDestAirportId(Integer destAirportId)         { this.destAirportId = destAirportId; }

    public Integer getCrsDepTime()                              { return crsDepTime; }
    public void setCrsDepTime(Integer crsDepTime)               { this.crsDepTime = crsDepTime; }

    public Double getDepDelay()                                 { return depDelay; }
    public void setDepDelay(Double depDelay)                    { this.depDelay = depDelay; }

    public Integer getCancelled()                               { return cancelled; }
    public void setCancelled(Integer cancelled)                 { this.cancelled = cancelled; }

    public Integer getDiverted()                                { return diverted; }
    public void setDiverted(Integer diverted)                   { this.diverted = diverted; }

    public boolean getHeartbeat()                               { return heartbeat; }
    public void setHeartbeat(boolean heartbeat)                 { this.heartbeat = heartbeat; }

    public long getKafkaProduceTime()                               { return kafkaProduceTime; }
    public void setKafkaProduceTime(long kafkaProduceTime)          { this.kafkaProduceTime = kafkaProduceTime; }

    @Override
    public String toString() {
        if (heartbeat) {
            return String.format("FlightEvent{HEARTBEAT, t=%d}", eventTime);
        }
        return String.format(
                "FlightEvent{t=%d, airline=%s, origin=%s, dest=%s, delay=%s, cancelled=%s, diverted=%s}",
                eventTime, airline, originAirportId, destAirportId,
                depDelay, cancelled, diverted
        );
    }
}