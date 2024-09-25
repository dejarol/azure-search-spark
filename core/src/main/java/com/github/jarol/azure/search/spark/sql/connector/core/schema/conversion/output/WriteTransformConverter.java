package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output;

import org.jetbrains.annotations.Nullable;

import java.util.Objects;

/**
 * Search property transformer that applies a transformation to a non-null Spark internal object
 * @param <T> output transformation type
 */

public abstract class WriteTransformConverter<T>
        implements WriteConverter {

    @Override
    public final @Nullable T apply(Object value) {

        // Transform the object if not null
        return Objects.isNull(value) ?
                null :
                transform(value);
    }

    /**
     * Apply the transformation
     * @param value Spark internal value
     * @return a Search document property
     */

    protected abstract T transform(Object value);
}
