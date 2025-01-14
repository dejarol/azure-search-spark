package com.github.jarol.azure.search.spark.sql.connector.read;

import com.azure.search.documents.models.SearchOptions;
import org.jetbrains.annotations.NotNull;

/**
 * Interface for generating {@link SearchOptions}
 */

@FunctionalInterface
public interface SearchOptionsSupplier {

    /**
     * Create the Search options
     * @return Search options
     */

    @NotNull SearchOptions createSearchOptions();
}
