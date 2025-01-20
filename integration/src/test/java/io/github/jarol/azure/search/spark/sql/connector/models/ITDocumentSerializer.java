package io.github.jarol.azure.search.spark.sql.connector.models;

import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Parent serializer for documents that extend {@link ITDocument}
 * @param <T> document type (should extend {@link ITDocument})
 */

public abstract class ITDocumentSerializer<T extends ITDocument>
        implements DocumentSerializer<T> {

    @Override
    public final @NotNull Map<String, Object> serialize(
            @NotNull T document
    ) {

        // Add 'id' property
        return extend(
                document,
                new LinkedHashMap<String, Object>() {{
                    put("id", document.id());
                }}
        );
    }

    /**
     * Extend the 'base' map (i.e. the map containing only the document id) with other document properties
     * @param document document
     * @param map base map
     * @return an enriched map of document properties
     */

    protected abstract @NotNull Map<String, Object> extend(
            @NotNull T document,
            @NotNull Map<String, Object> map
    );
}
