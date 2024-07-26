package com.github.jarol.azure.search.spark.sql.connector.read.partitioning;

public class SinglePartitionPartitioner
        implements SearchPartitioner {

    @Override
    public SearchPartition[] generatePartitions() {
        return new SearchPartition[0];
    }
}
