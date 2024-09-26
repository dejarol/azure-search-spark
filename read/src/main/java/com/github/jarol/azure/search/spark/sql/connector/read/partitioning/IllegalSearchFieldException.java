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

    /**
     * Create an instance for {@link SearchField} not enabled for a {@link SearchFieldFeature}
     * @param name field name
     * @param feature expected enabled feature
     * @return an instance
     */

    @Contract("_, _ -> new")
    public static @NotNull IllegalSearchFieldException notEnabledFor(
            String name,
            @NotNull SearchFieldFeature feature
    ) {

        return new IllegalSearchFieldException(
                name,
                String.format("not %s", feature.description()
                )
        );
    }

    /**
     * Create an instance for a non-existing field
     * @param name field name
     * @return an instance
     */

    @Contract("_ -> new")
    public static @NotNull IllegalSearchFieldException nonExisting(
            String name
    ) {
        return new IllegalSearchFieldException(
                name,
                "does not exist"
        );
    }

    /**
     * Create an instance for a {@link SearchField} whose type is not eligible for being a partitioning field
     * @param searchField Search field
     * @return an instance
     */

    @Contract("_ -> new")
    public static @NotNull IllegalSearchFieldException fieldTypeNotEligibleForPartitioning(
            @NotNull SearchField searchField
    ) {

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
