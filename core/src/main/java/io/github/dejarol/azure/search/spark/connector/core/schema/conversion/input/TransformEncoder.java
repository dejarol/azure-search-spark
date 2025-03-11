package io.github.dejarol.azure.search.spark.connector.core.schema.conversion.input;

import org.jetbrains.annotations.Nullable;

import java.util.Objects;

/**
 * Encoder that applies a transformation to a non-null Search object
 * @param <T> target Spark internal type
 */

public abstract class TransformEncoder<T>
        implements SearchEncoder {

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

