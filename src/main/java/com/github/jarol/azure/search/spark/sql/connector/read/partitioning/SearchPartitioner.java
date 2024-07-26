package com.github.jarol.azure.search.spark.sql.connector.read.partitioning;

@FunctionalInterface
public interface SearchPartitioner {

    SearchPartition[] generatePartitions();
}