package io.github.jarol.azure.search.spark.connector.core.schema.conversion;

/**
 * Common interface for Search-Spark schema violations.
 * <br>
 * Schema violations may occur due to
 * <ul>
 *     <li>missing schema fields</li>
 *     <li>namesake fields with incompatible data type</li>
 * </ul>
 */

public interface SchemaViolation {

    /**
     * Violation type
     */

    enum Type {

        MISSING_FIELD,
        INCOMPATIBLE_TYPE,
        INCOMPATIBLE_ARRAY_TYPE,
        INCOMPATIBLE_COMPLEX_FIELD,
        NOT_SUITABLE_AS_GEOPOINT
    }

    /**
     * Get the field name
     * @return field name
     */

    String getFieldName();

    /**
     * Get the violation type
     * @return violation type
     */

    Type getType();

    /**
     * Get a description in JSON format
     * @return description as JSON string
     */

    String description();
}
