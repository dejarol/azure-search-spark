package com.github.jarol.azure.search.spark.sql.connector.types.conversion;

@FunctionalInterface
public interface SparkInternalConverter {

    Object apply(Object value);
}
