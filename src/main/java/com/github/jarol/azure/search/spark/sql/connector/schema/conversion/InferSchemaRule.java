package com.github.jarol.azure.search.spark.sql.connector.schema.conversion;

/**
 * Conversion rule to use for schema inference
 */

public abstract class InferSchemaRule
        implements SearchSparkConversionRule {

    @Override
    public final boolean useForSchemaInference() {
        return true;
    }

    @Override
    public final boolean useForSchemaConversion() {
        return false;
    }
}
