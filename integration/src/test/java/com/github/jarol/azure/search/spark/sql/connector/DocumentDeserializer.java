package com.github.jarol.azure.search.spark.sql.connector;

import org.jetbrains.annotations.NotNull;

import java.util.Map;

/**
 * Deserializer for converting a {@link com.azure.search.documents.SearchDocument} to a Java/Scala model
 * @param <TValue> model type
 */

@FunctionalInterface
public interface DocumentDeserializer<TValue> {

    /**
     * Deserialize a document
     * @param document document
     * @return an instance of target type
     */

    TValue deserialize(
            @NotNull Map<String, Object> document
    );
}
