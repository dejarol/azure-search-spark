package io.github.jarol.azure.search.spark.sql.connector.core.schema;

import com.azure.search.documents.indexes.models.SearchField;
import org.jetbrains.annotations.Contract;

import java.util.Optional;
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
            "facetable",
            SearchField::setFacetable,
            SearchField::isFacetable
    ),

    /**
     * Feature for enabling a field to be filterable
     */

    FILTERABLE(
            "filterable",
            SearchField::setFilterable,
            SearchField::isFilterable
    ),

    /**
     * Feature for enabling a field to be hidden
     */

    HIDDEN(
            "hidden",
            SearchField::setHidden,
            SearchField::isHidden
    ),

    /**
     * Feature for enabling a field as document's key
     */

    KEY("key",
            SearchField::setKey,
            SearchField::isKey
    ),

    /**
     * Feature for enabling a field to be searchable
     */

    SEARCHABLE(
            "searchable",
            SearchField::setSearchable,
            SearchField::isSearchable
    ),

    /**
     * Feature for enabling a field to be sortable
     */

    SORTABLE(
            "sortable",
            SearchField::setSortable,
            SearchField::isSortable
    );

    private final String description;
    private final BiFunction<SearchField, Boolean, SearchField> enablingFunction;
    private final Function<SearchField, Boolean> featurePredicate;

    /**
     * Create an instance
     * @param description feature description
     * @param enablingFunction function for enabling or disabling a feature on a Search field
     * @param featurePredicate predicate for evaluating if the feature is enabled
     */

    SearchFieldFeature(
            String description,
            BiFunction<SearchField, Boolean, SearchField> enablingFunction,
            Function<SearchField, Boolean> featurePredicate
    ) {
        this.description = description;
        this.enablingFunction = enablingFunction;
        this.featurePredicate = featurePredicate;
    }

    /**
     * Get this feature description
     * @return description
     */

    @Contract(pure = true)
    public String description() {
        return description;
    }

    /**
     * Enable a feature on a {@link SearchField}
     * @param searchField Search field
     * @return input search field with this feature enabled
     */

    public SearchField enableOnField(
            SearchField searchField
    ) {


        return enablingFunction.apply(searchField, true);
    }

    /**
     * Disable a feature on a {@link SearchField}
     * @param searchField Search field
     * @return input search field with given feature disabled
     */

    public SearchField disableOnField(
            SearchField searchField
    ) {

        return enablingFunction.apply(searchField, false);
    }

    /**
     * Evaluate if the feature is enabled on a {@link SearchField}
     * @param searchField Search field
     * @return true for enabled features
     */

    public boolean isEnabledOnField(
            SearchField searchField
    ) {

        return Optional.ofNullable(
                featurePredicate.apply(searchField)
        ).orElse(false);
    }
}
