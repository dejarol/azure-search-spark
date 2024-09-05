package com.github.jarol.azure.search.spark.sql.connector.read;

import com.github.jarol.azure.search.spark.sql.connector.AzureSparkException;
import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.SearchPartition;
import org.apache.spark.sql.connector.read.InputPartition;
import org.jetbrains.annotations.NotNull;

/**
 * Exception returned when an unexpected {@link InputPartition} (other than {@link SearchPartition})
 * is provided to a dataSource {@link org.apache.spark.sql.connector.read.PartitionReaderFactory}
 */

public class UnexpectedPartitionTypeException
        extends AzureSparkException {

    /**
     * Create an instance for a partition class
     * @param actual actual {@link InputPartition} class
     */

    public UnexpectedPartitionTypeException(
            @NotNull Class<? extends InputPartition> actual
    ) {

        super(String.format(
                "Found a partition of type %s, expecting a %s",
                actual.getName(),
                SearchPartition.class.getName())
        );
    }
}
