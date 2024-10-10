package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input;

import org.apache.spark.unsafe.types.UTF8String;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;

/**
 * Collection of {@link ReadConverter}(s)
 */

public final class ReadConverters {

    public static final ReadTransformConverter<String> STRING_VALUE_OF;

    public static final ReadTransformConverter<UTF8String> UTF8_STRING;

    /**
     * Converter for int32
     */

    public static final ReadCastConverter<Integer> INT32;

    /**
     * Converter for int64
     */

    public static final ReadCastConverter<Long> INT64;

    /**
     * Converter for doubles
     */

    public static final ReadCastConverter<Double> DOUBLE;

    /**
     * Converter for floats
     */

    public static final ReadCastConverter<Float> SINGLE;

    /**
     * Converter for booleans
     */

    public static final ReadCastConverter<Boolean> BOOLEAN;

    /**
     * Converter for timestamps
     * (internally represented as epoch microseconds)
     */

    public static final ReadTimeConverter<Long> TIMESTAMP;


    /**
     * Converter for days
     * (internally represented as epoch days)
     */

    public static final ReadTimeConverter<Integer> DATE;

    static {

        STRING_VALUE_OF = new ReadTransformConverter<String>() {
            @Override
            protected String transform(Object value) {
                return String.valueOf(value);
            }
        };

        UTF8_STRING = new ReadTransformConverter<UTF8String>() {
            @Override
            protected UTF8String transform(Object value) {
                return UTF8String.fromString((String) value);
            }
        };

        INT32 = new ReadCastConverter<>();
        INT64 = new ReadCastConverter<>();
        DOUBLE = new ReadCastConverter<>();
        SINGLE = new ReadCastConverter<>();
        BOOLEAN = new ReadCastConverter<>();

        TIMESTAMP = new ReadTimeConverter<Long>() {

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

        DATE = new ReadTimeConverter<Integer>() {

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
