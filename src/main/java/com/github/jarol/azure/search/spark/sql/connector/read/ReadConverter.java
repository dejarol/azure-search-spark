package com.github.jarol.azure.search.spark.sql.connector.read;

/**
 * Converter from a property in a search document to a Spark internal value
 * @param <T> Spark internal value type
 */

@FunctionalInterface
public interface ReadConverter<T> {

    /**
     * Convert an object (a document property) into its Spark internal representation
     * @param obj a property within a search document
     * @return the equivalent Spark internal representation
     */

    T toInternal(Object obj);
}
