package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output;

public class WriteCastConverter<T>
        extends WriteTransformConverter<T> {

    @SuppressWarnings("unchecked")
    @Override
    protected T transform(Object value) {
        return (T) value;
    }
}
