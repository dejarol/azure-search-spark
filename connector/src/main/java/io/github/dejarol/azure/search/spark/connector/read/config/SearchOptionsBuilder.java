package io.github.dejarol.azure.search.spark.connector.read.config;

import com.azure.search.documents.models.SearchOptions;

/**
 * Interface for building query options to be used for querying documents
 */

public interface SearchOptionsBuilder {

    /**
     * Add an OData <code>\$filter</code> to this builder
     * @param other filter to add
     * @return this builder with a new filter to add on query options
     */

    SearchOptionsBuilder addFilter(
            String other
    );

    /**
     * Add a facet expression to this builder
     * @param facet facet to add
     * @return this builder with a new filter to add on query options
     */

    SearchOptionsBuilder addFacet(
            String facet
    );

    /**
     * Build the query options
     * @return query options
     */

    SearchOptions buildOptions();
}
