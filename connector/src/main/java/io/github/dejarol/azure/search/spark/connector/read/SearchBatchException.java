package io.github.dejarol.azure.search.spark.connector.read;

import io.github.dejarol.azure.search.spark.connector.core.Constants;
import io.github.dejarol.azure.search.spark.connector.read.partitioning.SearchPartition;
import io.github.dejarol.azure.search.spark.connector.read.partitioning.SearchPartitioner;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Exception raised by the {@link org.apache.spark.sql.connector.read.Scan} implementation of this dataSource
 */

public class SearchBatchException
        extends IllegalArgumentException {

    /**
     * Create an instance
     * @param message message
     */

    private SearchBatchException(
            String message
    ) {
        super(message);
    }

    /**
     * Create new instance, given a message and a cause
     * @param message message
     * @param cause cause
     */

    private SearchBatchException(
            String message,
            Throwable cause
    ) {

        super(message, cause);
    }

    /**
     * Create a new instance, due to some {@link SearchPartition} breaking the limit of retrievable documents
     * @param searchPartitions Search partitions that break the limit of retrievable documents per partition
     * @return a new exception instance
     */

    public static @NotNull SearchBatchException forInvalidPartitions(
            @NotNull List<SearchPartition> searchPartitions
    ) {

        String partitionDescription = searchPartitions.stream().map(
                partition -> String.format("id %s, filter '%s'",
                        partition.getPartitionId(),
                        partition.getPartitionFilter()
                )
        ).collect(Collectors.joining(", "));

        String message = String.format(
                "Found %s partition(s) breaking the limit of %s documents per partition (%s). " +
                        "Try to change the partitioner options, or use a different partitioner",
                searchPartitions.size(),
                Constants.DOCUMENTS_PER_PARTITION_LIMIT,
                partitionDescription
        );

        return new SearchBatchException(message);
    }

    /**
     * Create a new instance, due to an exception caught when trying to create a {@link SearchPartitioner} instance
     * @param cause cause
     * @return a new exception instance
     */

    @Contract("_ -> new")
    public static @NotNull SearchBatchException forFailedPartitionerCreation(
            @NotNull Throwable cause
    ) {

        String message = String.format(
                "Cannot create the partitioner instance. Reason: %s",
                cause.getMessage()
        );

        return new SearchBatchException(message, cause);
    }
}
