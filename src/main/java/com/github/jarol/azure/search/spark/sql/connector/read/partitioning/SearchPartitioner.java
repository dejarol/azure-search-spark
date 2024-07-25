package com.github.jarol.azure.search.spark.sql.connector.read.partitioning;

public interface SearchPartitioner {

    SearchPartition[] generatePartitions();
}