package io.github.jarol.azure.search.spark.sql.connector.read.partitioning;

import org.apache.spark.sql.connector.read.InputPartition;
import org.jetbrains.annotations.Nullable;

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
     * Get the partition filter (can be null if the partition does not define its own filter)
     * @return the partition filter
     */

    @Nullable String getPartitionFilter();
}
