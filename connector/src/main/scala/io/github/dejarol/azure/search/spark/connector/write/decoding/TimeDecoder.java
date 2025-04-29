package io.github.dejarol.azure.search.spark.connector.write.decoding;

import io.github.dejarol.azure.search.spark.connector.core.Constants;
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
     * Transform the Spark internal value to a datetime object with an offset
     * @param value Spark internal value
     * @return a datetime with an offset
     */

    protected abstract OffsetDateTime toOffsetDateTime(Object value);
}
