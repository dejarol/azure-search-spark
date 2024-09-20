package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output;

import com.github.jarol.azure.search.spark.sql.connector.core.Constants;
import org.jetbrains.annotations.NotNull;

import java.time.OffsetDateTime;

/**
 * Converter from Spark internal time types to Search datetime type
 */

public abstract class SearchPropertyTimeConverter
        extends SearchPropertyTransformConverter<String> {

    @Override
    final protected @NotNull String transform(Object value) {

        // Convert the Spark internal object to date time
        // and then format it as a string
        return toOffsetDateTime(value)
                .format(Constants.DATE_TIME_FORMATTER);
    }

    /**
     * Transform the Spark internal value to an {@link OffsetDateTime}
     * @param value Spark internal value
     * @return an {@link OffsetDateTime}
     */

    protected abstract OffsetDateTime toOffsetDateTime(Object value);
}
