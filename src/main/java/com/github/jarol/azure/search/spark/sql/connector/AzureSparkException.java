package com.github.jarol.azure.search.spark.sql.connector;

public class AzureSparkException
        extends RuntimeException {

    public AzureSparkException(
            String message
    ) {
        super(message);
    }

    public AzureSparkException(
            String message,
            Throwable cause
    ) {
        super(message, cause);
    }

    public AzureSparkException(
            Throwable cause
    ) {
        super(cause);
    }
}
