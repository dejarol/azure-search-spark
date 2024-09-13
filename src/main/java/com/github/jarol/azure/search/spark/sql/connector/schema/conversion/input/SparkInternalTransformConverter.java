package com.github.jarol.azure.search.spark.sql.connector.schema.conversion.input;

import org.jetbrains.annotations.Nullable;

import java.util.Objects;

/**
 * Spark internal converter that applies a transformation of the Search object
 * @param <T> Spark internal object type
 */

public abstract class SparkInternalTransformConverter<T>
        implements SparkInternalConverter {

    @Override
    public final @Nullable T toSparkInternalObject(Object value) {

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
