package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input;

import org.apache.spark.unsafe.types.UTF8String;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;

/**
 * Read converter that applies a transformation to a non-null Search object
 * @param <T> target Spark internal type
 */

public abstract class ReadTransformConverter<T>
        implements ReadConverter {

    public static final ReadTransformConverter<String> STRING_VALUE_OF;

    public static final ReadTransformConverter<UTF8String> UTF8_STRING;

    static {

        STRING_VALUE_OF = new ReadTransformConverter<String>() {
            @Override
            protected String transform(Object value) {
                return String.valueOf(value);
            }
        };

        UTF8_STRING = new ReadTransformConverter<UTF8String>() {
            @Override
            protected UTF8String transform(Object value) {
                return UTF8String.fromString((String) value);
            }
        };
    }

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
