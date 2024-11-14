package com.github.jarol.azure.search.spark.sql.connector;

import org.jetbrains.annotations.NotNull;

/**
 * Interface for retrieving the id of a document
 * @param <T> document type
 */

@FunctionalInterface
public interface DocumentIDGetter<T> {

    /**
     * Get the document id
     * @param document document
     * @return document id
     */

    @NotNull String getId(
            @NotNull T document
    );
}
