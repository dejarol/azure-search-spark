package io.github.dejarol.azure.search.spark.connector.write;

import com.azure.search.documents.indexes.models.SearchIndex;
import org.jetbrains.annotations.NotNull;

/**
 * Interface to extend in order to implement actions
 * to apply on a {@link com.azure.search.documents.indexes.models.SearchIndex}
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
