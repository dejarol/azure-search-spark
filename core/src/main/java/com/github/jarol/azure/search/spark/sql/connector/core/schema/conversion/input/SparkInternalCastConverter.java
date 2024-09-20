package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input;

public class SparkInternalCastConverter<T>
        implements SparkInternalConverter {

    @Override
    public final T toSparkInternalObject(Object value) {

        //noinspection unchecked
        return (T) value;
    }
}
