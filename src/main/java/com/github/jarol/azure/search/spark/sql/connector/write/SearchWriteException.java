package com.github.jarol.azure.search.spark.sql.connector.write;

import org.jetbrains.annotations.NotNull;

public class SearchWriteException
        extends RuntimeException {

    public SearchWriteException(
            @NotNull Throwable cause
    ) {
        super(String.format("Cannot build dataSource write. Reason %s",
                cause.getMessage()),
                cause
        );
    }
}
