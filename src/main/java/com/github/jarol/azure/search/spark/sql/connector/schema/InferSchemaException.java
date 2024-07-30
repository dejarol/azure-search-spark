package com.github.jarol.azure.search.spark.sql.connector.schema;

import com.github.jarol.azure.search.spark.sql.connector.AzureSparkException;

public class InferSchemaException
        extends AzureSparkException {

    public static final String COULD_NOT_INFER_SCHEMA_PREFIX = "Could not infer schema for index";

    public InferSchemaException(
            String name,
            String reason
    ) {
        super(String.format("%s %s (%s)",
                COULD_NOT_INFER_SCHEMA_PREFIX, name, reason)
        );
    }
}
