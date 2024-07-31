package com.github.jarol.azure.search.spark.sql.connector.read.partitioning;

import com.github.jarol.azure.search.spark.sql.connector.AzureSparkException;

import java.util.List;

@FunctionalInterface
public interface SearchPartitioner {

    /**
     * Generate a collection of not-overlapping {@link SearchPartition} that will be used for executing parallel searches on an index
     * @return a collection of Search partitions
     * @throws AzureSparkException if partitions generation fails
     */

    List<SearchPartition> createPartitions() throws AzureSparkException;
}