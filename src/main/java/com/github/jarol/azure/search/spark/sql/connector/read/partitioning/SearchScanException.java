package com.github.jarol.azure.search.spark.sql.connector.read.partitioning;

import com.github.jarol.azure.search.spark.sql.connector.AzureSparkException;

public class SearchScanException
        extends AzureSparkException {

    public SearchScanException(
            String reason
    ) {
        super(String.format(
                "Failed to create the Search scan. Reason: %s",
                reason)
        );
    }
}
