package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input;

import org.apache.spark.unsafe.types.UTF8String;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;

/**
 * Collection of {@link SearchEncoder}s for atomic data types
 */

public final class AtomicEncoders {

    /**
     * Noop encoder (returns a value as-is)
     */

    public static final SearchEncoder IDENTITY = value -> value;

    /**
     * Support encoder from numeric/boolean types to string
     */

    public static final SearchEncoder STRING_VALUE_OF;

    /**
     * Encoder for strings (internally represented as {@link UTF8String}
     */

    public static final SearchEncoder UTF8_STRING;

    /**
     * Encoder for timestamps (internally represented as epoch microseconds)
     */

    public static final SearchEncoder TIMESTAMP;


    /**
     * Encoder for dates (internally represented as epoch days)
     */

    public static final SearchEncoder DATE;

    static {

        STRING_VALUE_OF = new TransformEncoder<String>() {
            @Override
            protected String transform(Object value) {
                return String.valueOf(value);
            }
        };

        UTF8_STRING = new TransformEncoder<UTF8String>() {
            @Override
            protected UTF8String transform(Object value) {
                return UTF8String.fromString((String) value);
            }
        };

        TIMESTAMP = new TimeEncoder<Long>() {

            @Override
            protected @NotNull Long toInternalObject(
                    @NotNull OffsetDateTime dateTime
            ) {

                return ChronoUnit.MICROS.between(
                        Instant.EPOCH,
                        dateTime.toInstant()
                );
            }
        };

        DATE = new TimeEncoder<Integer>() {

            @Override
            protected Integer toInternalObject(
                    OffsetDateTime dateTime
            ) {

                return Long.valueOf(
                        dateTime.toLocalDate().toEpochDay()
                ).intValue();
            }
        };
    }
}
