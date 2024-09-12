package com.github.jarol.azure.search.spark.sql.connector.read.partitioning;

import com.azure.search.documents.models.SearchOptions;
import org.apache.spark.sql.connector.read.InputPartition;

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

    String getSearchFilter();

    /**
     * Get the list of fields to be retrieved from Search documents.
     * If null or empty, all possible fields will be retrieved
     * @return list of fields to select
     */

    List<String> getSearchSelect();

    /**
     * Return the search options that will be used for retrieving documents belonging to this partition
     * @return search options for retrieving the partition documents
     */

    default SearchOptions getSearchOptions() {

        List<String> select = getSearchSelect();
        String[] selectArray = Objects.isNull(select) || select.isEmpty()?
                null :
                select.toArray(new String[0]);

        return new SearchOptions()
                .setFilter(getSearchFilter())
                .setSelect(selectArray);
    }
}
