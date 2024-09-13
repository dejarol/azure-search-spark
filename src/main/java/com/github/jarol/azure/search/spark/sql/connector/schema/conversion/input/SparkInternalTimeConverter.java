package com.github.jarol.azure.search.spark.sql.connector.schema.conversion.input;

import com.github.jarol.azure.search.spark.sql.connector.Constants;

import java.time.OffsetDateTime;

/**
 * Spark internal converter for time-related types, i.e.
 * <ul>
 *     <li>dates</li>
 *     <li>timestamps</li>
 * </ul>
 * @param <T> converter output type
 */

public abstract class SparkInternalTimeConverter<T>
        extends SparkInternalTransformConverter<T> {

    @Override
    protected final T transform(Object value) {

        // Convert to OffsetDateTime and then transform
        return dateTimeToInternalObject(
                OffsetDateTime.parse(
                        (String) value,
                        Constants.DATE_TIME_FORMATTER
                )
        );
    }

    /**
     * Convert an instance of {@link OffsetDateTime} to a Spark internal object
     * @param dateTime offset date time
     * @return a Spark internal object
     */

    protected abstract T dateTimeToInternalObject(OffsetDateTime dateTime);
}
