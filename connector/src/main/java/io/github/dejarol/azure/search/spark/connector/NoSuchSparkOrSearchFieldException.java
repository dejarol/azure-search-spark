package io.github.dejarol.azure.search.spark.connector;

import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.function.Supplier;

/**
 * Exception thrown when one or more Spark/Search fields does not exist
 */

public class NoSuchSparkOrSearchFieldException
        extends IllegalArgumentException {

    /**
     * Constructor
     * @param message message supplier
     */

    private NoSuchSparkOrSearchFieldException(
            @NotNull Supplier<String> message
    ) {
        super(message.get());
    }

    /**
     * Convenience factory method for creating a {@link NoSuchSparkOrSearchFieldException} for a single non-existing field
     * @param fieldName name of the non-existing field
     * @return a new instance
     */

    public static @NotNull NoSuchSparkOrSearchFieldException forSingleField(
            @NotNull String fieldName
    ) {

        return new NoSuchSparkOrSearchFieldException(
                () -> String.format(
                        "Field %s does not exist",
                        fieldName
                )
        );
    }

    /**
     * Convenience factory method for creating a {@link NoSuchSparkOrSearchFieldException} for multiple non-existing fields
     * @param fieldNames names of the non-existing fields
     * @return a new instance
     */

    public static @NotNull NoSuchSparkOrSearchFieldException forFields(
            @NotNull List<String> fieldNames
    ) {

        return new NoSuchSparkOrSearchFieldException(
                () -> String.format(
                        "Fields [%s] do not exist",
                        String.join(", ", fieldNames)
                )
        );
    }
}
