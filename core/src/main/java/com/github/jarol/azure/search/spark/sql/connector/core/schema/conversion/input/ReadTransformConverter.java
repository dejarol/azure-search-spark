package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input;

import org.jetbrains.annotations.Nullable;

import java.util.Objects;

/**
 * Spark internal converter that applies a transformation of the Search object
 * @param <T> Spark internal object type
 */

public abstract class ReadTransformConverter<T>
        implements ReadConverter {

    @Override
    public final @Nullable T apply(Object value) {

       return Objects.isNull(value) ?
               null :
               transform(value);
    }

    /**
     * Applies the transformation on a non-null Search object
     * @param value search object
     * @return a Spark internal object
     */

    protected abstract T transform(Object value);
}
