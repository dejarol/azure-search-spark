package com.github.jarol.azure.search.spark.sql.connector.core.utils;

import com.azure.core.util.Context;
import com.azure.search.documents.SearchClient;
import com.azure.search.documents.indexes.SearchIndexClient;
import com.azure.search.documents.models.SearchOptions;
import com.azure.search.documents.util.SearchPagedIterable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Collection of Java-based utilities for interacting with Azure Search clients
 */

public final class SearchUtils {

    /**
     * Return a {@link SearchPagedIterable} by querying a Search client using some query options
     * @param client Search client
     * @param searchText search text
     * @param searchOptions Search options
     * @return a {@link SearchPagedIterable}
     */

    public static SearchPagedIterable getSearchPagedIterable(
            @NotNull SearchClient client,
            @Nullable String searchText,
            SearchOptions searchOptions
    ) {

        return client.search(
                searchText,
                searchOptions,
                Context.NONE
        );
    }

    /**
     * Evaluate if a Search index exists
     * @param client client
     * @param name index name
     * @return true for existing indexes
     */

    public static boolean indexExists(
            @NotNull SearchIndexClient client,
            String name
    ) {

        return client.listIndexes().stream()
                .anyMatch(searchIndex ->
                        searchIndex.getName().equalsIgnoreCase(name)
                );
    }
}
