package io.github.dejarol.azure.search.spark.connector.read.partitioning;

import com.azure.search.documents.indexes.models.SearchField;
import io.github.dejarol.azure.search.spark.connector.core.schema.SearchFieldFeature;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

/**
 * Exception raised due to existing (but invalid) candidate facet fields
 * <br>
 * An existing Search field is a good candidate for faceting if
 * <ul>
 *     <li>is a string or numeric field</li>
 *     <li>is both facetable and filterable</li>
 * </ul>
 */

public class IllegalFacetableFieldException
        extends IllegalSearchFieldException {

    /**
     * Private constructor
     * @param  searchField Search field
     * @param reasonSupplier supplier for reason message
     */

    private IllegalFacetableFieldException(
            @NotNull SearchField searchField,
            @NotNull Supplier<String> reasonSupplier
    ) {
        super(() -> String.format(
                "Invalid facetable field (%s, type %s). %s",
                searchField.getName(),
                searchField.getType(),
                reasonSupplier.get())
        );
    }

    /**
     * Create a new instance for a field with invalid type (i.e. not string or numeric)
     * @param searchField candidate Search field
     * @return a new instance
     */

    @Contract("_ -> new")
    public static @NotNull IllegalFacetableFieldException forInvalidType(
            @NotNull SearchField searchField
    ) {

        return new IllegalFacetableFieldException(
                searchField,
                () -> "Only string or numeric types are supported"
        );
    }

    /**
     * Create a new instance for a field that is not enabled for faceting or filtering
     * @param searchField candidate Search field
     * @param feature missing feature
     * @return a new instance
     */

    @Contract("_, _ -> new")
    public static @NotNull IllegalFacetableFieldException forMissingFeature(
            @NotNull SearchField searchField,
            @NotNull SearchFieldFeature feature
    ) {

        return new IllegalFacetableFieldException(
                searchField,
                () -> String.format("Field is not %s. It should be both %s and %s",
                        feature.description(),
                        SearchFieldFeature.FACETABLE.description(),
                        SearchFieldFeature.FILTERABLE.description())
        );
    }
}
