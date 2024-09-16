package com.github.jarol.azure.search.spark.sql.connector;

/**
 * Exception thrown for illegal data type states
 */

public class DataTypeException
        extends IllegalStateException {

    /**
     * Create an instance with a message
     * @param message message
     */

    public DataTypeException(
            String message
    ) {
        super(message);
    }
}
