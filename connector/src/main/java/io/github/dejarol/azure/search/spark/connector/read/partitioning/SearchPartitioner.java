package io.github.dejarol.azure.search.spark.connector.read.partitioning;

import java.util.List;

/**
 * Search partition generator.
 * It will be used for generating input partitions for parallel data read
 */

@FunctionalInterface
public interface SearchPartitioner {

    String FACET_FIELD_CONFIG = "facetField";
    String NUM_PARTITIONS_CONFIG = "numPartitions";
    String PARTITION_FIELD_CONFIG = "partitionField";
    String LOWER_BOUND_CONFIG = "lowerBound";
    String UPPER_BOUND_CONFIG = "upperBound";

    /**
     * Generate a collection of not-overlapping {@link SearchPartition} that will be used for executing
     * parallel read operations on an index
     * @return a collection of Search partitions
     */

    List<SearchPartition> createPartitions();
}