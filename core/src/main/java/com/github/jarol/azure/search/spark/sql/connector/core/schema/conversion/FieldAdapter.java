package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion;

import org.apache.spark.sql.types.DataType;

/**
 * Interface that adapts a {@link DataType} to a more manageable API
 */

public interface FieldAdapter {

    /**
     * Get the fields name
     * @return field name
     */

    String name();

    /**
     * Get the field Spark type
     * @return field Spark type
     */

    DataType sparkType();

    int index();
}
