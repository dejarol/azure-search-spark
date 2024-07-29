package com.github.jarol.azure.search.spark.sql.connector.read.partitioning;

import java.util.List;

@FunctionalInterface
public interface SearchPartitioner {

    List<SearchPartition> generatePartitions();
}