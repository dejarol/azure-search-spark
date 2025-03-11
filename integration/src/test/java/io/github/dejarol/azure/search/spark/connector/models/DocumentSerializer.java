package io.github.dejarol.azure.search.spark.connector.models;

import org.jetbrains.annotations.NotNull;

import java.util.Map;

/**
 * Serializer for transforming objects into an equivalent Map representation,
 * representing the object properties as a {@link com.azure.search.documents.SearchDocument}
 * @param <TDocument> object type
 */

@FunctionalInterface
public interface DocumentSerializer<TDocument> {

    /**
     * Serialize an instance as a set of document properties
     * @param document a document instance
     * @return a map with document properties
     */

    Map<String, Object> serialize(
            @NotNull TDocument document
    );
}
