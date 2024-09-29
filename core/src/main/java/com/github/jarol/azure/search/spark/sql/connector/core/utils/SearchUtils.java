package com.github.jarol.azure.search.spark.sql.connector.core.utils;

import com.azure.core.util.Context;
import com.azure.search.documents.SearchClient;
import com.azure.search.documents.models.SearchOptions;
import com.azure.search.documents.util.SearchPagedIterable;
import org.jetbrains.annotations.NotNull;

public final class SearchUtils {

    /**
     * Return a {@link SearchPagedIterable}, eventually including the total count of retrieved documents
     * @param client Search client
     * @param searchOptions Search options
     * @return a {@link SearchPagedIterable}
     */

    public static SearchPagedIterable getSearchPagedIterable(
            @NotNull SearchClient client,
            SearchOptions searchOptions
    ) {

        return client.search(
                null,
                searchOptions,
                Context.NONE
        );
    }
}
