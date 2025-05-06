package io.github.dejarol.azure.search.spark.connector.read.partitioning;

import io.github.dejarol.azure.search.spark.connector.read.config.ReadConfig;

import java.util.Collections;

/**
 * Simple Java implementation of a {@link PartitionerFactory} used for testing
 */

public class EmptyJavaPartitionerFactory
        implements PartitionerFactory {

    @Override
    public SearchPartitioner createPartitioner(ReadConfig readConfig) {

        return Collections::emptyList;
    }
}
