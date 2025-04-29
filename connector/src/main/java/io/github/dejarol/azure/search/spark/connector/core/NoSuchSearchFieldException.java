package io.github.dejarol.azure.search.spark.connector.core;

import org.jetbrains.annotations.NotNull;

/**
 * Exception raised when a Search field does not exist
 */

public class NoSuchSearchFieldException
        extends IllegalArgumentException {

  /**
   * Creates a new instance for a non-existent Search field
   * @param name field name
   */

    public NoSuchSearchFieldException(
            @NotNull String name
    ) {
        super(
                String.format(
                        "Field %s does not exist",
                        name
                )
        );
    }
}
