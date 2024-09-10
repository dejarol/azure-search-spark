package com.github.jarol.azure.search.spark.sql.connector.read.partitioning;

import com.azure.search.documents.models.SearchOptions;
import org.apache.spark.sql.connector.read.InputPartition;

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
     * Return the search options that will be used for retrieving documents that will belong to this partition
     * @return search options for retrieving the partition documents
     */

    SearchOptions getSearchOptions();
}
