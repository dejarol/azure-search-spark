package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion;

/**
 * Schema conversion rule
 */

public abstract class SchemaConversionRule
        implements SearchSparkConversionRule {

    @Override
    public final boolean useForSchemaInference() {
        return false;
    }

    @Override
    public final boolean useForSchemaConversion() {
        return true;
    }
}
