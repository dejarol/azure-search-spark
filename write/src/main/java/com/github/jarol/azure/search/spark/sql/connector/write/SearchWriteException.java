package com.github.jarol.azure.search.spark.sql.connector.write;

import org.jetbrains.annotations.NotNull;

/**
 * Exception thrown when building the dataSource {@link org.apache.spark.sql.connector.write.BatchWrite}
 */

public class SearchWriteException
        extends RuntimeException {

    /**
     * Create a new instance
     * @param cause cause
     */

    public SearchWriteException(
            @NotNull Throwable cause
    ) {
        super(String.format("Cannot build dataSource write. Reason %s",
                cause.getMessage()),
                cause
        );
    }
}
