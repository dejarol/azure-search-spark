package io.github.dejarol.azure.search.spark.connector.read.partitioning;

import com.azure.search.documents.indexes.models.SearchField;
import io.github.dejarol.azure.search.spark.connector.core.schema.SearchFieldFeature;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

/**
 * Exception for existing (but invalid) partitioning field
 * <br>
 * An existing field is a candidate for partitioning if
 * <ul>
 *     <li>is either a numeric or datetime field</li>
 *     <li>is filterable</li>
 * </ul>
 */

public class IllegalPartitioningFieldException
        extends IllegalArgumentException {

    /**
     * Private constructor
     * @param searchField Search field
     * @param supplier reason message supplier
     */

    private IllegalPartitioningFieldException(
            @NotNull SearchField searchField,
            @NotNull Supplier<String> supplier
    ) {
        super(
                String.format(
                        "Invalid partitioning field (%s, type %s). %s",
                        searchField.getName(),
                        searchField.getType(),
                        supplier.get()
                )
        );
    }

    /**
     * Create a new instance for a non-filterable field
     * @param searchField candidate Search field
     * @return a new instance
     */

    @Contract("_ -> new")
    public static @NotNull IllegalPartitioningFieldException forNonFilterableField(
            @NotNull SearchField searchField
    ) {

        return new IllegalPartitioningFieldException(
                searchField,
                () -> String.format("Field is not %s",
                        SearchFieldFeature.FILTERABLE.description()
                )
        );
    }

    /**
     * Create a new instance for a candidate field with invalid type
     * @param searchField candidate Search field
     * @return a new instance
     */

    @Contract("_ -> new")
    public static @NotNull IllegalPartitioningFieldException forNonPartitionableType(
            @NotNull SearchField searchField
    ) {

        return new IllegalPartitioningFieldException(
                searchField,
                () -> "Only numeric or datetime fields are supported"
        );
    }
}
