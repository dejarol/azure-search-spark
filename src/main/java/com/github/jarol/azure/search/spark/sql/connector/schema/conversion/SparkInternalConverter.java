package com.github.jarol.azure.search.spark.sql.connector.schema.conversion;

@FunctionalInterface
public interface SparkInternalConverter {

    Object toSparkInternalObject(Object value);
}
