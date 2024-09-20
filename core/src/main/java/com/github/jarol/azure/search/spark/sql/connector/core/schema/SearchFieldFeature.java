package com.github.jarol.azure.search.spark.sql.connector.core.schema;

import com.azure.search.documents.indexes.models.SearchField;
import org.jetbrains.annotations.Contract;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Features that can be enabled or disabled on a {@link SearchField}
 */

public enum SearchFieldFeature {

    /**
     * Feature for enabling a field to be facetable
     */

    FACETABLE(
            SearchField::setFacetable,
            SearchField::isFacetable
    ),

    /**
     * Feature for enabling a field to be filterable
     */

    FILTERABLE(
            SearchField::setFilterable,
            SearchField::isFilterable
    ),

    /**
     * Feature for enabling a field to be hidden
     */

    HIDDEN(
            SearchField::setHidden,
            SearchField::isHidden
    ),

    /**
     * Feature for enabling a field as document's key
     */

    KEY(
            SearchField::setKey,
            SearchField::isKey
    ),

    /**
     * Feature for enabling a field to be searchable
     */

    SEARCHABLE(
            SearchField::setSearchable,
            SearchField::isSearchable
    ),

    /**
     * Feature for enabling a field to be sortable
     */

    SORTABLE(
            SearchField::setSortable,
            SearchField::isSortable
    );

    private final BiFunction<SearchField, Boolean, SearchField> enablingFunction;
    private final Function<SearchField, Boolean> featurePredicate;

    /**
     * Create an instance
     * @param enablingFunction function for enabling or disabling a feature on a Search field
     * @param featurePredicate predicate for evaluating if the feature is enabled
     */

    @Contract(pure = true)
    SearchFieldFeature(
            BiFunction<SearchField, Boolean, SearchField> enablingFunction,
            Function<SearchField, Boolean> featurePredicate
    ) {
        this.enablingFunction = enablingFunction;
        this.featurePredicate = featurePredicate;
    }

    /**
     * Enable the feature on a {@link SearchField}
     * @param searchField Search field
     * @return input search field with this feature enabled
     */

    public SearchField enable(
            SearchField searchField
    ) {

        return enablingFunction.apply(searchField, true);
    }

    /**
     * Evaluate if the feature is enabled on a {@link SearchField}
     * @param searchField Search field
     * @return true for enabled features
     */

    public boolean isEnabled(
            SearchField searchField
    ) {

        Boolean test = featurePredicate.apply(searchField);
        return Objects.nonNull(test) && test;
    }
}
