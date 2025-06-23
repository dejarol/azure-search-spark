package io.github.dejarol.azure.search.spark.connector.core.utils;

import com.azure.core.util.Context;
import com.azure.search.documents.SearchClient;
import com.azure.search.documents.indexes.SearchIndexClient;
import com.azure.search.documents.indexes.models.SearchIndex;
import com.azure.search.documents.models.SearchOptions;
import com.azure.search.documents.util.SearchPagedIterable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Collection of Java-based utilities for interacting with Azure Search clients
 */

public final class SearchClients {

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
     * List all existing Search indexes
     * @param client Search client
     * @return a list of Search indexes
     * @since 0.11.0
     */

    public static List<SearchIndex> listIndexes(
            @NotNull SearchIndexClient client
    ) {

        return client.listIndexes()
                .stream()
                .collect(Collectors.toList());
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
