package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion;

import org.apache.spark.sql.types.DataType;

/**
 * A Search index column for collecting both Search field and Spark column specifications
 */

public interface SearchIndexColumn {

    /**
     * Get the field's name
     * @return field name
     */

    String name();

    /**
     * Get field's Spark type
     * @return field's Spark type
     */

    DataType sparkType();

    /**
     * Gets the index of the column within the schema
     * @return column index
     */

    int index();
}
