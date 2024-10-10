package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input;

/**
 * Read converters that will transform an object by casting it to a target Spark internal type
 * @param <T> target Spark internal type
 */

public final class ReadCastConverter<T>
        extends ReadTransformConverter<T> {

    @SuppressWarnings("unchecked")
    @Override
    protected T transform(Object value) {
        return (T) value;
    }
}
