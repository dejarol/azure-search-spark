package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input;

import com.github.jarol.azure.search.spark.sql.connector.core.Constants;

import java.time.OffsetDateTime;

/**
 * Spark internal converter for time-related types, i.e.
 * <ul>
 *     <li>dates</li>
 *     <li>timestamps</li>
 * </ul>
 * @param <T> converter output type
 */

public abstract class ReadTimeConverter<T>
        extends ReadTransformConverter<T> {

    @Override
    protected final T transform(Object value) {

        // Convert to OffsetDateTime and then transform
        return dateTimeToInternalObject(
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

    protected abstract T dateTimeToInternalObject(OffsetDateTime dateTime);
}
