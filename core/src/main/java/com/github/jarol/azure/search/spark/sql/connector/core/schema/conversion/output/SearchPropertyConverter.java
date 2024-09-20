package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output;

/**
 * Converter from a Spark internal object to Search document property
 */

@FunctionalInterface
public interface SearchPropertyConverter {

    /**
     * Convert a Spark internal value to a Search document property
     * @param value internal value
     * @return a Search document property
     */

    Object toSearchProperty(Object value);
}
