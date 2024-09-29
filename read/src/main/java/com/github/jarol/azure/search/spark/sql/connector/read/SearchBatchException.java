package com.github.jarol.azure.search.spark.sql.connector.read;

import com.github.jarol.azure.search.spark.sql.connector.core.Constants;
import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.SearchPartition;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.stream.Collectors;

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
     * Create a new instance, due to some {@link SearchPartition} breaking the
     * limit of retrievable documents
     * (see this link <a href="https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.search.models.searchparameters.skip?view=azure-dotnet-legacy">
     *     SearchParameters.Skip Property</a>}
     * @param searchPartitions Search partitions that break the limit of retrievable documents per partition
     * @return a new exception instance
     */

    public static @NotNull SearchBatchException forInvalidPartitions(
            @NotNull List<SearchPartition> searchPartitions
    ) {

        String partitionDescription = searchPartitions.stream().map(
                partition -> String.format("id %s, filter '%s'",
                        partition.getPartitionId(),
                        partition.getSearchFilter())
        ).collect(Collectors.joining(", "));

        String message = String.format(
                "Found %s partition(s) breaking the limit of %s documents per partition (%s). " +
                        "Try to change the partitioner options",
                searchPartitions.size(),
                Constants.PARTITION_DOCUMENT_LIMIT,
                partitionDescription
        );

        return new SearchBatchException(message);
    }
}
