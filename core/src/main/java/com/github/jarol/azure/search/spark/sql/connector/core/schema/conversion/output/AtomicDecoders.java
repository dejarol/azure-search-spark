package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output;

import com.github.jarol.azure.search.spark.sql.connector.core.Constants;
import org.apache.spark.unsafe.types.UTF8String;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/**
 * Collection of atomic {@link SearchDecoder}
 */

public final class AtomicDecoders {

    /**
     * Gets a no-op decoder (i.e. a decoder that does not apply any transformation)
     * @return a no-op decoder
     */

    @Contract(pure = true)
    public static @NotNull SearchDecoder identity() {

        return value -> value;
    }

    /**
     * Get the decoder for strings
     * @return decoder for strings
     */

    @Contract(value = " -> new", pure = true)
    public static @NotNull SearchDecoder forUTF8Strings() {

        // Strings are internally represented as UTF8Strings
        return new TransformDecoder<String>() {
            @Override
            protected String transform(Object value) {
                return new String(
                        ((UTF8String) value).getBytes(),
                        StandardCharsets.UTF_8
                );
            }
        };
    }

    /**
     * Gets a decoder from numeric/boolean types to string
     * @return a decoder from numeric/boolean types to string
     */

    @Contract(value = " -> new", pure = true)
    public static @NotNull SearchDecoder stringValueOf() {

        return new TransformDecoder<String>() {
            @Override
            protected String transform(Object value) {
                return String.valueOf(value);
            }
        };
    }

    /**
     * Gets a decoder for dates
     * @return decoder for dates
     */

    @Contract(value = " -> new", pure = true)
    public static @NotNull SearchDecoder forDates() {

        return new TimeDecoder() {
            @Override
            protected OffsetDateTime toOffsetDateTime(Object value) {

                return OffsetDateTime.of(
                        LocalDate.ofEpochDay((Integer) value),
                        LocalTime.MIDNIGHT,
                        Constants.UTC_OFFSET
                );
            }
        };
    }

    /**
     * Gets a decoder from dates to strings
     * @return decoder from dates to strings
     */

    @Contract(value = " -> new", pure = true)
    public static @NotNull SearchDecoder fromDateToString() {

        TransformDecoder<String> toIsoLocalDate = new TransformDecoder<String>() {

            @Override
            protected @NotNull String transform(Object value) {
                return OffsetDateTime.parse(
                        (String) value,
                        Constants.DATETIME_OFFSET_FORMATTER
                ).format(DateTimeFormatter.ISO_LOCAL_DATE);
            }
        };

        return forDates().andThen(toIsoLocalDate);
    }

    /**
     * Gets a decoder for timestamps
     * @return decoder for timestamps
     */

    @Contract(value = " -> new", pure = true)
    public static @NotNull SearchDecoder forTimestamps() {

        return new TimeDecoder() {
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
