package com.github.jarol.azure.search.spark.sql.connector.schema.conversion;

import com.azure.search.documents.indexes.models.SearchFieldDataType;
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.input.SparkInternalConverter;
import org.apache.spark.sql.types.DataType;

/**
 * A conversion rule from a Search dataType to Spark internal dataType
 * <br>
 * It defines
 * <ul>
 *     <li>the related Search data type</li>
 *     <li>the related Spark data type</li>
 *     <li>a converter from a Search object to a Spark internal object</li>
 * </ul>
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
