package com.github.jarol.azure.search.spark.sql.connector.write;

import org.jetbrains.annotations.NotNull;

public class IndexCreationException
        extends RuntimeException {

    public IndexCreationException(
            String name,
            @NotNull Throwable cause
    ) {
        super(String.format(
                "Failed to create index %s. Reason: %s",
                name,
                cause.getMessage()),
                cause
        );
    }
}
