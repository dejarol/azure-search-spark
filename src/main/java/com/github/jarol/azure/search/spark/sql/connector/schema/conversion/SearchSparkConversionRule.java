package com.github.jarol.azure.search.spark.sql.connector.schema.conversion;

import com.azure.search.documents.indexes.models.SearchFieldDataType;
import org.apache.spark.sql.types.DataType;

/**
 * Rule for converting Search ecosystem dataTypes to Spark internal dataTypes
 */

public interface SearchSparkConversionRule {

    /**
     * The Spark output dataType of this conversion rule
     * @return a Spark dataType
     */

    DataType sparkType();

    /**
     * The Search dataType related to this conversion rule
     * @return a Search dataType
     */

    SearchFieldDataType searchType();

    /**
     * Whether this rule should be used for schema inference
     * @return true for infer schema rules
     */

    boolean useForSchemaInference();

    /**
     * Whether this rule should be used for schema conversion
     * @return true for schema conversion rules
     */

    boolean useForSchemaConversion();

    /**
     * Get a converter instance
     * @return this rule's converter instance
     */

    SparkInternalConverter converter();
}
