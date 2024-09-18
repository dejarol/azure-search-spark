package com.github.jarol.azure.search.spark.sql.connector.schema.conversion;

import com.azure.search.documents.indexes.models.SearchFieldDataType;
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.input.SparkInternalConverter;
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.output.SearchPropertyConverter;
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
     * Evaluate if this instance accepts a Spark type
     * @param other Spark type
     * @return true if this instance's Spark type equals given type
     */

    default boolean acceptsSparkType(DataType other) {

        return sparkType().equals(other);
    }

    /**
     * Evaluate if this instance accepts a SearchType
     * @param other SearchType
     * @return true if this instance's SearchType equals given type
     */

    default boolean acceptsSearchType(SearchFieldDataType other) {

        return searchType().equals(other);
    }

    /**
     * Evaluate if this instance accepts a Spark type and a Search type
     * @param otherDatatype spark type
     * @param otherSearchType search type
     * @return true if both this instance's Spark type and Search type accept given types
     */

    default boolean acceptsTypes(DataType otherDatatype, SearchFieldDataType otherSearchType) {

        return sparkType().acceptsType(otherDatatype) &&
                acceptsSearchType(otherSearchType);
    }

    /**
     * Get this instance's Spark converter
     * @return rule's Spark converter instance
     */

    SparkInternalConverter sparkConverter();

    /**
     * Get this instance's Search converter
     * @return rule's Search converter instance
     */

    SearchPropertyConverter searchConverter();
}
