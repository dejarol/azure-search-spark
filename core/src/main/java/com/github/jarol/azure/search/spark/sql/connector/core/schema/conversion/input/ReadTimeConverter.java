package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input;

import com.github.jarol.azure.search.spark.sql.connector.core.Constants;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;

/**
 * Read converter for time-related types, i.e.
 * <ul>
 *     <li>dates</li>
 *     <li>timestamps</li>
 * </ul>
 * @param <T> converter output type
 */

public abstract class ReadTimeConverter<T>
        extends ReadTransformConverter<T> {

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

    @Override
    protected final T transform(Object value) {

        // Convert to OffsetDateTime and then transform
        return toInternalObject(
                OffsetDateTime.parse(
                        (String) value,
                        Constants.DATETIME_OFFSET_FORMATTER
                )
        );
    }

    /**
     * Convert an instance of {@link OffsetDateTime} to a Spark internal object
     * @param dateTime offset date time
     * @return a Spark internal object
     */

    protected abstract T toInternalObject(OffsetDateTime dateTime);
}
