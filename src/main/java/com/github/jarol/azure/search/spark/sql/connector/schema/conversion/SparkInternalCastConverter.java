package com.github.jarol.azure.search.spark.sql.connector.schema.conversion;

public abstract class SparkInternalCastConverter<T>
        implements SparkInternalConverter {

    @Override
    public final T toSparkInternalObject(Object value) {

        //noinspection unchecked
        return (T) value;
    }
}
