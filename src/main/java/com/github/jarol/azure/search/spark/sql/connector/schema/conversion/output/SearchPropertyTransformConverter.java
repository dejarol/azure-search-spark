package com.github.jarol.azure.search.spark.sql.connector.schema.conversion.output;

import org.jetbrains.annotations.Nullable;

import java.util.Objects;

public abstract class SearchPropertyTransformConverter<T>
        implements SearchPropertyConverter {

    @Override
    public final @Nullable T toSearchProperty(Object value) {

        return Objects.isNull(value) ?
                null :
                transform(value);
    }

    protected abstract T transform(Object value);
}
