package it.uniroma2.sabd.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Utilità per la formattazione dell'output CSV.
 *
 * Centralizza la conversione epoch ms → stringa leggibile e la
 * formattazione dei valori double, evitando duplicazioni tra Q1 e Q2.
 */
public class CsvOutputFormatter {

    /** Formato timestamp identico agli esempi di output della traccia. */
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Converte un epoch ms in stringa "yyyy-MM-dd HH:mm:ss" (UTC).
     * Es.: 1735725600000L → "2025-01-01 08:00:00"
     */
    public static String formatTimestamp(long epochMs) {
        LocalDateTime ldt = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(epochMs), ZoneOffset.UTC);
        return ldt.format(FORMATTER);
    }

    /**
     * Formatta un valore double con 2 cifre decimali.
     * Se il valore è Double.NaN o non disponibile, restituisce "null".
     * Es.: 11.4232 → "11.42"
     */
    public static String formatDouble(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) return "null";
        return String.format("%.2f", value);
    }

    /**
     * Formatta un valore double nullable.
     * Se null, restituisce "null"; altrimenti formatta con 2 decimali.
     */
    public static String formatDouble(Double value) {
        if (value == null) return "null";
        return formatDouble(value.doubleValue());
    }

    /**
     * Formatta una percentuale (0.0–100.0) con 2 cifre decimali.
     * Es.: 2.4193548... → "2.42"
     */
    public static String formatPercent(double value) {
        return String.format("%.2f", value);
    }
}