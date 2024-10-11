package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output;

import com.github.jarol.azure.search.spark.sql.connector.core.Constants;
import org.apache.spark.unsafe.types.UTF8String;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;

public class AtomicDecoders {

    /**
     * Decoder for strings
     */
    
    public static final SearchDecoder STRING;

    /**
     * Decoder for dates
     */

    public final static SearchDecoder DATE;

    /**
     * Decoder for timestamps
     */

    public final static SearchDecoder TIMESTAMP;

    static {

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
