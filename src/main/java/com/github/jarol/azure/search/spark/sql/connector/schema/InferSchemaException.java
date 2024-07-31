package com.github.jarol.azure.search.spark.sql.connector.schema;

import com.github.jarol.azure.search.spark.sql.connector.AzureSparkException;

/**
 * Exception raised during schema inference
 */

public class InferSchemaException
        extends AzureSparkException {

    public static final String COULD_NOT_INFER_SCHEMA_PREFIX = "Could not infer schema for index";

    /**
     * Create an instance related to an index, reporting also the reason
     * @param name  index name
     * @param reason reason
     */

    public InferSchemaException(
            String name,
            String reason
    ) {
        super(String.format("%s %s (%s)",
                COULD_NOT_INFER_SCHEMA_PREFIX, name, reason)
        );
    }
}
