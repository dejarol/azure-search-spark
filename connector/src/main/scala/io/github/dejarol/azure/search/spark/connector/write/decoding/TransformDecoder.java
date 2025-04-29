package io.github.dejarol.azure.search.spark.connector.write.decoding;

import org.jetbrains.annotations.Nullable;

import java.util.Objects;

/**
 * Decoder that applies a transformation to a non-null Spark internal value
 * @param <T> Search target data type
 */

public abstract class TransformDecoder<T>
        implements SearchDecoder {

    @Override
    public final @Nullable T apply(Object value) {

        return Objects.isNull(value) ?
                null:
                transform(value);
    }

    /**
     * Applies the transformation to a non-null Spark internal value
     * @param value non-null value
     * @return the decoded value
     */

    protected abstract T transform(Object value);
}
