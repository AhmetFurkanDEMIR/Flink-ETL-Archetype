package org.demir.utils;

import org.apache.flink.table.data.TimestampData;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.time.format.DateTimeFormatter;

public class ParseTimestamp {

    public static TimestampData parseTimestamp(String timestampStr) {
        if (timestampStr == null || timestampStr.isEmpty() || timestampStr.equalsIgnoreCase("null")) {
            return null;
        }

        try {
            // Z ile biten UTC formatı
            if (timestampStr.endsWith("Z")) {
                Instant instant = Instant.parse(timestampStr);
                LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
                return TimestampData.fromLocalDateTime(dateTime);
            } else if (timestampStr.contains("T") && timestampStr.contains("+")) {
                // Mikro/saniye hassasiyetli ofset içeren format (örn: 2025-05-05T10:14:09.8843797+03:00)
                DateTimeFormatter formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
                OffsetDateTime offsetDateTime = OffsetDateTime.parse(timestampStr, formatter);
                LocalDateTime dateTime = offsetDateTime.toLocalDateTime();
                return TimestampData.fromLocalDateTime(dateTime);
            } else if (timestampStr.contains("T")) {
                // ISO 8601 formatı (örn: 2025-05-05T09:50:00)
                DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
                LocalDateTime dateTime = LocalDateTime.parse(timestampStr, formatter);
                return TimestampData.fromLocalDateTime(dateTime);
            } else {
                // Ofset içeren format
                OffsetDateTime offsetDateTime = OffsetDateTime.parse(timestampStr);
                LocalDateTime dateTime = offsetDateTime.toLocalDateTime();
                return TimestampData.fromLocalDateTime(dateTime);
            }
        } catch (DateTimeParseException e) {
            // Hatalı format
            System.err.println("dateTime Error: " + timestampStr);
        }

        return null;
    }
}
