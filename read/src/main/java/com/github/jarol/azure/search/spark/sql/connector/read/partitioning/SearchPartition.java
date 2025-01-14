package com.github.jarol.azure.search.spark.sql.connector.read.partitioning;

import com.azure.search.documents.SearchClient;
import com.azure.search.documents.models.SearchOptions;
import com.azure.search.documents.models.SearchResult;
import com.azure.search.documents.util.SearchPagedIterable;
import com.github.jarol.azure.search.spark.sql.connector.core.utils.SearchUtils;
import org.apache.spark.sql.connector.read.InputPartition;
import org.jetbrains.annotations.NotNull;

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
     * @param includeTotalCount true for including the total document count
     * @return a {@link SearchPagedIterable}
     */

    default SearchPagedIterable getSearchPagedIterable(
            @NotNull SearchClient client,
            boolean includeTotalCount
    ) {

        return SearchUtils.getSearchPagedIterable(
                client,
                getSearchOptions().setIncludeTotalCount(includeTotalCount)
        );
    }

    /**
     * Return an iterator with retrieved {@link SearchResult}(s)
     * @param searchClient Search client
     * @return iterator fo {@link SearchResult}(s)
     */

    default Iterator<SearchResult> getPartitionResults(
            @NotNull SearchClient searchClient
    ) {

        return getSearchPagedIterable(
                searchClient,
                false
        ).iterator();
    }

    /**
     * Get the number of results retrieved by this partition
     * @param searchClient Search client
     * @return the number of results retrieved by this partition
     */

    default Long getCountPerPartition(
            @NotNull SearchClient searchClient
    ) {

        return getSearchPagedIterable(
                searchClient,
                true
        ).getTotalCount();
    }
}
