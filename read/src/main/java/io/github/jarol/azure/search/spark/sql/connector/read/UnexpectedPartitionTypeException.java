package io.github.jarol.azure.search.spark.sql.connector.read;

import io.github.jarol.azure.search.spark.sql.connector.read.partitioning.SearchPartition;
import org.apache.spark.sql.connector.read.InputPartition;
import org.jetbrains.annotations.NotNull;

/**
 * Exception returned when an unexpected {@link InputPartition} (other than {@link SearchPartition})
 * is provided to this dataSource's {@link org.apache.spark.sql.connector.read.PartitionReaderFactory}
 */

public class UnexpectedPartitionTypeException
        extends IllegalArgumentException {

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
