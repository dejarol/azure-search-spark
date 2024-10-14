package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output;

import com.github.jarol.azure.search.spark.sql.connector.core.Constants;
import org.apache.spark.unsafe.types.UTF8String;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class AtomicDecoders {

    public static final SearchDecoder IDENTITY;

    /**
     * Decoder for strings
     */
    
    public static final SearchDecoder STRING;

    /**
     * Decoder from numeric/boolean types to string
     */

    public static final SearchDecoder STRING_VALUE_OF;

    /**
     * Decoder for dates
     */

    public final static SearchDecoder DATE;

    public final static SearchDecoder DATE_TO_STRING;

    /**
     * Decoder for timestamps
     */

    public final static SearchDecoder TIMESTAMP;

    static {

        IDENTITY = value -> value;

        STRING_VALUE_OF = new TransformDecoder<String>() {
            @Override
            protected String transform(Object value) {
                return String.valueOf(value);
            }
        };

        STRING = new TransformDecoder<String>() {
            @Override
            protected String transform(Object value) {
                return new String(
                        ((UTF8String) value).getBytes(),
                        StandardCharsets.UTF_8
                );
            }
        };

        DATE = new TimeDecoder() {
            @Override
            protected OffsetDateTime toOffsetDateTime(Object value) {

                return OffsetDateTime.of(
                        LocalDate.ofEpochDay((Integer) value),
                        LocalTime.MIDNIGHT,
                        Constants.UTC_OFFSET
                );
            }
        };

        DATE_TO_STRING = new TransformDecoder<String>() {
            @Override
            protected String transform(Object value) {
                return ((Date) value)
                        .toLocalDate()
                        .format(DateTimeFormatter.ISO_LOCAL_DATE);
            }
        };

        TIMESTAMP = new TimeDecoder() {
            @Override
            protected OffsetDateTime toOffsetDateTime(Object value) {
                return Instant.EPOCH.plus(
                        (Long) value,
                        ChronoUnit.MICROS
                ).atOffset(Constants.UTC_OFFSET);
            }
        };
    }
}
