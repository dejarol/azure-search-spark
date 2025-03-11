package io.github.dejarol.azure.search.spark.connector.core.schema.conversion.input;

import org.apache.spark.unsafe.types.UTF8String;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;

/**
 * Collection of {@link SearchEncoder}s for atomic data types
 */

public final class AtomicEncoders {

    /**
     * Gets a no-op encoder (i.e. no operation is applied)
     * @return a no-op encoder
     */

    @Contract(pure = true)
    public static @NotNull SearchEncoder identity() {

        return value -> value;
    }

    /**
     * Gets an encoder that converts an object to string by invoking Java's {@link String#valueOf} method
     * @return encoder implementing string conversion
     */

    @Contract(value = " -> new", pure = true)
    public static @NotNull SearchEncoder stringValueOf() {

        return new TransformEncoder<String>() {
            @Override
            protected String transform(Object value) {
                return String.valueOf(value);
            }
        };
    }

    /**
     * Gets an encoder for converting Search strings to Spark internal strings (represented as {@link UTF8String})
     * @return encoder for Spark internal strings
     */

    @Contract(value = " -> new", pure = true)
    public static @NotNull SearchEncoder forUTF8Strings() {

        return new TransformEncoder<UTF8String>() {
            @Override
            protected UTF8String transform(Object value) {
                return UTF8String.fromString((String) value);
            }
        };
    }

    /**
     * Gets an encoder for Spark timestamp
     * @return encoder for timestamps
     */

    @Contract(value = " -> new", pure = true)
    public static @NotNull SearchEncoder forTimestamps() {

        return new TimeEncoder<Long>() {

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
    }

    /**
     * Gets an encoder for Spark dates
     * @return encoder for dates
     */

    @Contract(value = " -> new", pure = true)
    public static @NotNull SearchEncoder forDates() {

        return new TimeEncoder<Integer>() {

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
