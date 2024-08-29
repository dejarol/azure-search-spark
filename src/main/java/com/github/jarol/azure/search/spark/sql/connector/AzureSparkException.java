package com.github.jarol.azure.search.spark.sql.connector;

/**
 * Parent class for all exceptions
 */

public class AzureSparkException
        extends RuntimeException {

    /**
     * Create a new instance with no message or cause
     */

    public AzureSparkException() {}

    /**
     * Create a new instance with a custom message
     * @param message message
     */

    public AzureSparkException(
            String message
    ) {
        super(message);
    }

    /**
     * Create a new instance with a custom message and a cause
     * @param message message
     * @param cause cause
     */

    public AzureSparkException(
            String message,
            Throwable cause
    ) {
        super(message, cause);
    }
}
