package com.github.jarol.azure.search.spark.sql.connector.write;

import com.azure.search.documents.indexes.models.SearchIndex;
import org.jetbrains.annotations.NotNull;

/**
 * Interface to extend in order to implement actions
 * to apply on a {@link SearchIndex}
 */

@FunctionalInterface
public interface SearchIndexAction {

    /**
     * Apply this instance's action to an index
     * @param index index
     * @return the input index, transformed by this instance's action
     */

    @NotNull SearchIndex apply(
            @NotNull SearchIndex index
    );
}
