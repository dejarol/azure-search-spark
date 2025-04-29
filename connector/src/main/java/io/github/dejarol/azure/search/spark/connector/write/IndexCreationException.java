package io.github.dejarol.azure.search.spark.connector.write;

import org.jetbrains.annotations.NotNull;

/**
 * Exception thrown when the target index could not be created
 */

public class IndexCreationException
        extends RuntimeException {

    /**
     * Create a new instance
     * @param name index name
     * @param cause cause
     */

    public IndexCreationException(
            @NotNull String name,
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
