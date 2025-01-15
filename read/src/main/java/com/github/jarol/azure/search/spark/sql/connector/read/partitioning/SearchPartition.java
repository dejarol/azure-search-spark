package com.github.jarol.azure.search.spark.sql.connector.read.partitioning;

import com.azure.search.documents.SearchClient;
import com.azure.search.documents.models.SearchOptions;
import com.azure.search.documents.models.SearchResult;
import com.azure.search.documents.util.SearchPagedIterable;
import com.github.jarol.azure.search.spark.sql.connector.core.utils.SearchUtils;
import org.apache.spark.sql.connector.read.InputPartition;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;

/**
 * A Search partition
 */

public interface SearchPartition
        extends InputPartition {

    /**
     * Get the partition id
     * @return partition id
     */

    int getPartitionId();

    /**
     * Return the search options that will be used for retrieving documents belonging to this partition
     * @return search options for retrieving the partition documents
     */

    SearchOptions getSearchOptions();

    /**
     * Return a {@link SearchPagedIterable}, eventually including the total count of retrieved documents
     * @param client Search client
     * @param searchText text to Search
     * @param includeTotalCount true for including the total document count
     * @return a {@link SearchPagedIterable}
     */

    default SearchPagedIterable getSearchPagedIterable(
            @NotNull SearchClient client,
            @Nullable String searchText,
            boolean includeTotalCount
    ) {

        return SearchUtils.getSearchPagedIterable(
                client,
                searchText,
                getSearchOptions().setIncludeTotalCount(includeTotalCount)
        );
    }

    /**
     * Return an iterator with retrieved {@link SearchResult}(s)
     * @param searchClient Search client
     * @param searchText text to Search
     * @return iterator fo {@link SearchResult}(s)
     */

    default Iterator<SearchResult> getPartitionResults(
            @NotNull SearchClient searchClient,
            @Nullable String searchText
    ) {

        return getSearchPagedIterable(
                searchClient,
                searchText,
                false
        ).iterator();
    }

    /**
     * Get the number of results retrieved by this partition
     * @param searchClient Search client
     * @param searchText text to Search
     * @return the number of results retrieved by this partition
     */

    default Long getCountPerPartition(
            @NotNull SearchClient searchClient,
            @Nullable String searchText
    ) {

        return getSearchPagedIterable(
                searchClient,
                searchText,
                true
        ).getTotalCount();
    }
}
