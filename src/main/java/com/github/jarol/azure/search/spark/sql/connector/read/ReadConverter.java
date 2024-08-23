package com.github.jarol.azure.search.spark.sql.connector.read;

import java.util.function.Function;

/**
 * Converter from a property in a search document to a Spark internal value
 * @param <T> Spark internal value type
 */

@FunctionalInterface
public interface ReadConverter<T>
        extends Function<Object, T> {
}
