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
 *
 * Flink richiede che i POJO abbiano:
 *  - costruttore no-arg pubblico
 *  - getter e setter pubblici per ogni campo
 *  - campi pubblici oppure getter/setter (usiamo getter/setter per chiarezza)
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
     * Usato in Q3 per ricavare la fascia oraria; presente nel messaggio
     * per completezza anche se Q3 non è richiesta per studenti singoli.
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
     * Il Producer lo trasmette come Int8 → deserializzato come Integer,
     * poi convertito in boolean tramite isCancelled().
     */
    private Integer cancelled;

    /** Flag di deviazione: 1 = deviato, 0 = non deviato. */
    private Integer diverted;

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
    }

    // ─── Metodi di convenienza (semantica di dominio) ─────────────────────────

    /**
     * Restituisce true se il volo è stato cancellato.
     * Un volo con campo cancelled null è considerato non cancellato
     * (scelta conservativa: il dato mancante non esclude il volo).
     */
    public boolean isCancelled() {
        return cancelled != null && cancelled == 1;
    }

    /**
     * Restituisce true se il volo è stato deviato.
     */
    public boolean isDiverted() {
        return diverted != null && diverted == 1;
    }

    /**
     * Restituisce true se il volo è "completato": né cancellato né deviato.
     * Usato sia in Q1 (conteggio voli completati) sia in Q2 (filtro soglia ritardo).
     */
    public boolean isCompleted() {
        return !isCancelled() && !isDiverted();
    }

    /**
     * Restituisce true se il volo ha un ritardo significativo in partenza (> 30 min).
     * Prerequisito: il volo deve essere completato (non cancellato, non deviato).
     * Usato in Q2.
     */
    public boolean hasSevereDelay() {
        return isCompleted() && depDelay != null && depDelay > 30.0;
    }

    /**
     * Restituisce true se il volo è in ritardo in partenza (> 15 min).
     * Prerequisito: il volo non deve essere cancellato.
     * Usato in Q1 per calcolare il tasso di partenze in ritardo.
     */
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

    @Override
    public String toString() {
        return String.format(
                "FlightEvent{t=%d, airline=%s, origin=%s, dest=%s, delay=%s, cancelled=%s, diverted=%s}",
                eventTime, airline, originAirportId, destAirportId,
                depDelay, cancelled, diverted
        );
    }
}