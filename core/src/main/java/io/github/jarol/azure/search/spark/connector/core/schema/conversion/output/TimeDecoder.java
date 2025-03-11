package io.github.jarol.azure.search.spark.connector.core.schema.conversion.output;

import io.github.jarol.azure.search.spark.connector.core.Constants;
import org.jetbrains.annotations.NotNull;

import java.time.OffsetDateTime;

/**
 * Decoder from Spark internal time types to Search datetime type
 */

public abstract class TimeDecoder
        extends TransformDecoder<String> {

    @Override
    final protected @NotNull String transform(Object value) {

        // Convert the Spark internal object to date time
        // and then format it as a string
        return toOffsetDateTime(value)
                .format(Constants.DATETIME_OFFSET_FORMATTER);
    }

    /**
     * Transform the Spark internal value to an {@link OffsetDateTime}
     * @param value Spark internal value
     * @return an {@link OffsetDateTime}
     */

    protected abstract OffsetDateTime toOffsetDateTime(Object value);
}
