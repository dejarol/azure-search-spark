package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output;

public class WriteCastConverter<T>
        implements WriteConverter {

    @Override
    public final T apply(Object value) {
        //noinspection unchecked
        return (T) value;
    }
}
