package io.github.dejarol.azure.search.spark.connector.core.schema.conversion.input;

import io.github.dejarol.azure.search.spark.connector.core.Constants;

import java.time.OffsetDateTime;

/**
 * Encoder for time-related types, i.e.
 * <ul>
 *     <li>dates</li>
 *     <li>timestamps</li>
 * </ul>
 * @param <T> converter output type
 */

public abstract class TimeEncoder<T>
        extends TransformEncoder<T> {

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
     * Convert an instance of datetime object to a Spark internal object
     * @param dateTime offset date time
     * @return a Spark internal object
     */

    protected abstract T toInternalObject(OffsetDateTime dateTime);
}
