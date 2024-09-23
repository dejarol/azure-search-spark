package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input;

public class ReadCastConverter<T>
        implements ReadConverter {

    @Override
    public final T toSparkInternalObject(Object value) {

        //noinspection unchecked
        return (T) value;
    }
}
