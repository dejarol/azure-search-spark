package io.github.dejarol.azure.search.spark.connector.core;

import org.jetbrains.annotations.NotNull;

/**
 * Exception for non-existing Search indexes
 */

public class IndexDoesNotExistException
        extends IllegalArgumentException {

    /**
     * Create an instance for given index name
     * @param name index name
     */

    public IndexDoesNotExistException(
            @NotNull String name
    ) {

        super(String.format(
                "Index %s does not exist",
                name)
        );
    }
}
