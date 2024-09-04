package com.github.jarol.azure.search.spark.sql.connector.schema.conversion;

import java.util.Objects;

public abstract class SparkInternalTransformConverter<T>
        implements SparkInternalConverter {

    @Override
    public final T toSparkInternalObject(Object value) {

       return Objects.isNull(value) ?
               null :
               transform(value);
    }

    protected abstract T transform(Object value);
}
