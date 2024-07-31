package com.github.jarol.azure.search.spark.sql.connector.clients;

import com.azure.core.credential.AzureKeyCredential;
import com.azure.core.util.Context;
import com.azure.search.documents.SearchClient;
import com.azure.search.documents.SearchClientBuilder;
import com.azure.search.documents.indexes.SearchIndexClient;
import com.azure.search.documents.indexes.SearchIndexClientBuilder;
import com.azure.search.documents.indexes.models.SearchIndex;
import com.azure.search.documents.models.SearchOptions;
import com.azure.search.documents.util.SearchPagedIterable;
import com.github.jarol.azure.search.spark.sql.connector.config.IOConfig;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

public final class ClientFactory {

    /**
     * Create a {@link SearchIndexClient}
     * @param config instance of {@link IOConfig}
     * @return an index client
     */

    @Contract("_ -> new")
    public static @NotNull SearchIndexClient searchIndexClient(
            @NotNull IOConfig config
    ) {
        return new SearchIndexClientBuilder()
                .endpoint(config.getEndpoint())
                .credential(new AzureKeyCredential(config.getAPIkey()))
                .buildClient();
    }

    /**
     * Create a client for interacting with a search index
     * @param config instance of {@link IOConfig}
     * @return a client for interacting with a search index
     */

    @Contract("_ -> new")
    public static @NotNull SearchIndex searchIndex(
            @NotNull IOConfig config
    ) {
        return searchIndexClient(config)
                .getIndex(config.getIndex());
    }

    /**
     * Create a client for performing search operations
     * @param config instance of {@link IOConfig}
     * @return a client for performing search operations
     */

    @Contract("_ -> new")
    public static @NotNull SearchClient searchClient(
            @NotNull IOConfig config
    ) {

        return new SearchClientBuilder()
                .endpoint(config.getEndpoint())
                .credential(new AzureKeyCredential(config.getAPIkey()))
                .indexName(config.getIndex())
                .buildClient();
    }

    public static SearchPagedIterable doSearch(
            IOConfig config,
            SearchOptions searchOptions
    ) {

        return searchClient(config)
                .search(null, searchOptions, Context.NONE);
    }
}
