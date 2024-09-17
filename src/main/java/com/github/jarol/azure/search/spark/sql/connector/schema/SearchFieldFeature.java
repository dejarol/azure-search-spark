package com.github.jarol.azure.search.spark.sql.connector.schema;

import com.azure.search.documents.indexes.models.SearchField;
import org.jetbrains.annotations.Contract;

import java.util.function.BiFunction;

/**
 * Features that can be enabled of disabled on a {@link SearchField}
 */

public enum SearchFieldFeature {

    /**
     * Feature for enabling a field to be facetable
     */

    FACETABLE(SearchField::setFacetable),

    /**
     * Feature for enabling a field to be filterable
     */

    FILTERABLE(SearchField::setFilterable),

    /**
     * Feature for enabling a field to be hidden
     */

    HIDDEN(SearchField::setHidden),

    /**
     * Feature for enabling a field as document's key
     */

    KEY(SearchField::setKey),

    /**
     * Feature for enabling a field to be searchable
     */

    SEARCHABLE(SearchField::setSearchable),

    /**
     * Feature for enabling a field to be sortable
     */

    SORTABLE(SearchField::setSortable);

    private final BiFunction<SearchField, Boolean, SearchField> enablingFunction;

    /**
     * Create an instance
     * @param enablingFunction function for enabling or disabling a feature on a Search field
     */

    @Contract(pure = true)
    SearchFieldFeature(
            BiFunction<SearchField, Boolean, SearchField> enablingFunction
    ) {
        this.enablingFunction = enablingFunction;
    }

    /**
     * Get the function to use for enabling/disabling a feature
     * @return function for enabling/disabling a feature
     */

    @Contract(pure = true)
    public BiFunction<SearchField, Boolean, SearchField> enablingFunction() {
        return enablingFunction;
    }
}
