package com.github.jarol.azure.search.spark.sql.connector.read.partitioning;

import com.azure.search.documents.models.SearchOptions;
import org.apache.spark.sql.connector.read.InputPartition;

import java.util.List;

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

    String getFilter();

    /**
     * Get the list of fields to select from retrieved documents.
     * If null, all retrievable fields will be selected
     * @return the fields to select
     */

    List<String> getSelect();

    /**
     * Return the search options that will be used for retrieving documents that will belong to this partition
     * @return search options for retrieving the partition documents
     */

    SearchOptions getSearchOptions();
}
