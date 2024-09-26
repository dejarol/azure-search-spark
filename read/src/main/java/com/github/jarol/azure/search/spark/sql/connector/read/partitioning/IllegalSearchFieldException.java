package com.github.jarol.azure.search.spark.sql.connector.read.partitioning;

import com.azure.search.documents.indexes.models.SearchField;
import com.github.jarol.azure.search.spark.sql.connector.core.schema.SearchFieldFeature;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

/**
 * Exception thrown for an illegal Search field
 */

public class IllegalSearchFieldException
        extends IllegalArgumentException {

    /**
     * Create an instance
     * @param name field name
     */

    public IllegalSearchFieldException(
            String name,
            String reasonPhrase
    ) {
        super(String.format(
                "Illegal search field (%s). Reason: %s",
                name, reasonPhrase
                )
        );
    }

    @Contract("_, _ -> new")
    public static @NotNull IllegalSearchFieldException notEnabledFor(
            String name,
            @NotNull SearchFieldFeature feature
    ) {

        // TODO: Javadoc
        return new IllegalSearchFieldException(
                name,
                String.format("not %s", feature.description()
                )
        );
    }

    @Contract("_ -> new")
    public static @NotNull IllegalSearchFieldException nonExisting(
            String name
    ) {
        // TODO: Javadoc
        return new IllegalSearchFieldException(
                name,
                "does not exist"
        );
    }

    @Contract("_ -> new")
    public static @NotNull IllegalSearchFieldException fieldTypeNotEligibleForPartitioning(
            @NotNull SearchField searchField
    ) {
        // TODO: Javadoc
        return new IllegalSearchFieldException(
                searchField.getName(),
                String.format(
                        "unsupported field type for partitioning (%s). " +
                                "Only numeric or datetime types are supported",
                        searchField.getType()
                )
        );
    }
}
