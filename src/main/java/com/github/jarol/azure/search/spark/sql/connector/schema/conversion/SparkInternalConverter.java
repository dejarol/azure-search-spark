package com.github.jarol.azure.search.spark.sql.connector.schema.conversion;

/**
 * A converter from a Search data object to a Spark internal object
 */

@FunctionalInterface
public interface SparkInternalConverter {

    /**
     * Convert a Search data object to a Spark internal object
     * @param value search data object
     * @return a Spark internal object
     */

    Object toSparkInternalObject(Object value);
}
