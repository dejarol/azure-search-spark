package com.github.jarol.azure.search.spark.sql.connector.write;

public class WriteBuilderException
        extends RuntimeException {

    public WriteBuilderException(
            Throwable cause
    ) {
        super("Cannot create the Write instance");
    }
}
