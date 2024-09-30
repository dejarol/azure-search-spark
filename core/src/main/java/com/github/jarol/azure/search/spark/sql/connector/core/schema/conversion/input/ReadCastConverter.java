package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input;

public class ReadCastConverter<T>
        extends ReadTransformConverter<T> {

    @SuppressWarnings("unchecked")
    @Override
    protected T transform(Object value) {
        return (T) value;
    }
}
