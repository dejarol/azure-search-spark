package com.github.jarol.azure.search.spark.sql.connector;

import com.azure.search.documents.SearchClient;
import com.azure.search.documents.SearchDocument;
import com.azure.search.documents.indexes.SearchIndexClient;
import com.azure.search.documents.indexes.models.IndexDocumentsBatch;
import com.azure.search.documents.indexes.models.SearchIndex;
import com.azure.search.documents.models.IndexAction;
import com.azure.search.documents.models.IndexActionType;
import com.azure.search.documents.models.SearchOptions;
import com.github.jarol.azure.search.spark.sql.connector.core.utils.SearchUtils;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility for Java-based integration test methods
 */

public final class SearchTestUtils {

    /**
     * List of existing indexes
     * @param client Search index client
     * @return list of existing index names
     */

    public static List<String> listIndexes(
            @NotNull SearchIndexClient client
    ) {

        return client.listIndexes()
                .stream().map(SearchIndex::getName)
                .collect(Collectors.toList());
    }

    /**
     * Read all documents
     * @param client Search client
     * @return a collection with all index document
     */

    public static List<SearchDocument> readDocuments(
            SearchClient client
    ) {

        return SearchUtils.getSearchPagedIterable(client, new SearchOptions())
                .stream().map(result -> result.getDocument(SearchDocument.class))
                .collect(Collectors.toList());
    }

    /**
     * Write a collection of documents
     * @param searchClient client for writing documents
     * @param documents documents
     * @param serializer serializer for documents
     * @param <TDocument> document type
     */

    public static <TDocument> void writeDocuments(
            @NotNull SearchClient searchClient,
            @NotNull List<TDocument> documents,
            @NotNull DocumentSerializer<TDocument> serializer
    ) {

        // Create one action for each document
        List<IndexAction<SearchDocument>> actions = documents.stream().map(
                d -> new IndexAction<SearchDocument>()
                        .setDocument(new SearchDocument(serializer.serialize(d)))
                        .setActionType(IndexActionType.UPLOAD)
        ).collect(Collectors.toList());

        // Create the batch and index documents
        IndexDocumentsBatch<SearchDocument> batch = new IndexDocumentsBatch<SearchDocument>().addActions(actions);
        searchClient.indexDocuments(batch);
    }
}
