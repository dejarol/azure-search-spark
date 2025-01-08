package com.github.jarol.azure.search.spark.sql.connector.read.partitioning;

import com.azure.search.documents.SearchClient;
import com.azure.search.documents.models.SearchOptions;
import com.azure.search.documents.models.SearchResult;
import com.azure.search.documents.util.SearchPagedIterable;
import com.github.jarol.azure.search.spark.sql.connector.core.utils.SearchUtils;
import com.github.jarol.azure.search.spark.sql.connector.read.filter.ODataExpression;
import org.apache.spark.sql.connector.read.InputPartition;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

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
     * Get the filter that would be applied by this instance for retrieving search documents.
     * If null, no filter will be applied to documents search
     * @return the search filter
     */

    String getODataFilter();

    /**
     * Get the list of fields to be retrieved from Search documents.
     * If null or empty, all possible fields will be retrieved
     * @return list of fields to select
     */

    List<String> getSelectedFields();

    /**
     * Get the collection of predicates that can be pushed down to this partition
     * @return predicates eligible for pushdown
     */

    ODataExpression[] getPushedPredicates();

    /**
     * Return the search options that will be used for retrieving documents belonging to this partition
     * @return search options for retrieving the partition documents
     */

    default SearchOptions getSearchOptions() {

        List<String> select = getSelectedFields();
        String[] selectArray = Objects.isNull(select) || select.isEmpty() ?
                null :
                select.toArray(new String[0]);

        return new SearchOptions()
                .setFilter(getODataFilter())
                .setSelect(selectArray);
    }

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
