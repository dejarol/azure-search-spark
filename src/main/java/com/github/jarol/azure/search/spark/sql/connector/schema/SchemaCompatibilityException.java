package com.github.jarol.azure.search.spark.sql.connector.schema;

import com.github.jarol.azure.search.spark.sql.connector.AzureSparkException;
import org.jetbrains.annotations.NotNull;

public class SchemaCompatibilityException
        extends AzureSparkException {

    public SchemaCompatibilityException(
            @NotNull String message
    ) {
        super(message);
    }
}
