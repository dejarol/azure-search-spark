package com.github.jarol.azure.search.spark.sql.connector.schema.conversion.output;

import com.github.jarol.azure.search.spark.sql.connector.Constants;
import org.jetbrains.annotations.NotNull;

import java.time.OffsetDateTime;

public abstract class SearchPropertyTimeConverter
        extends SearchPropertyTransformConverter<String> {

    @Override
    final protected @NotNull String transform(Object value) {

        return toOffsetDateTime(value)
                .format(Constants.DATE_TIME_FORMATTER);
    }

    protected abstract OffsetDateTime toOffsetDateTime(Object value);
}
