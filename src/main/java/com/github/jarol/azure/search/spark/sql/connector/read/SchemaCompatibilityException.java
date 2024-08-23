package com.github.jarol.azure.search.spark.sql.connector.read;

import com.github.jarol.azure.search.spark.sql.connector.AzureSparkException;

public class SchemaCompatibilityException
        extends AzureSparkException {

    public SchemaCompatibilityException(
            String reason
    ) {
        super(String.format(
                "Incompatible schema. Reason: %s",
                reason
        ));
    }
}
