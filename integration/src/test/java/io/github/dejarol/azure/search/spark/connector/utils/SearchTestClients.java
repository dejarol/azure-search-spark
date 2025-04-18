package io.github.dejarol.azure.search.spark.connector.utils;

import com.azure.search.documents.SearchClient;
import com.azure.search.documents.SearchDocument;
import com.azure.search.documents.indexes.SearchIndexClient;
import com.azure.search.documents.indexes.models.IndexDocumentsBatch;
import com.azure.search.documents.indexes.models.SearchIndex;
import com.azure.search.documents.models.IndexAction;
import com.azure.search.documents.models.IndexActionType;
import com.azure.search.documents.models.SearchOptions;
import com.azure.search.documents.util.SearchPagedIterable;
import io.github.dejarol.azure.search.spark.connector.core.utils.SearchClients;
import io.github.dejarol.azure.search.spark.connector.models.DocumentSerializer;
import io.github.dejarol.azure.search.spark.connector.read.partitioning.SearchPartition;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Utility for Java-based integration test methods
 */

public final class SearchTestClients {

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
     * Read documents from an index
     * @param client Search client
     * @param searchOptions search options (can be null if not required)
     * @return a collection with all index document
     */

    public static List<SearchDocument> readDocuments(
            @NotNull SearchClient client,
            @Nullable SearchOptions searchOptions,
            @Nullable String searchText
    ) {

        SearchOptions inputOptions = Objects.isNull(searchOptions) ? new SearchOptions() : searchOptions;
        return SearchClients.getSearchPagedIterable(client, searchText, inputOptions)
                .stream().map(result -> result.getDocument(SearchDocument.class))
                .collect(Collectors.toList());
    }

    /**
     * Read all documents
     * @param client Search client
     * @return a collection with all index document
     */

    public static List<SearchDocument> readAllDocuments(
            @NotNull SearchClient client
    ) {

        return readDocuments(client, null, null);
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

    /**
     * Get the set of documents retrieved by a {@link SearchPartition}
     * @param partition partition
     * @param client Search client
     * @return the documents for this given partition
     */

    public static List<SearchDocument> getPartitionDocuments(
            @NotNull SearchPartition partition,
            @NotNull SearchClient client
    ) {

        SearchPagedIterable iterable = SearchClients.getSearchPagedIterable(
                client,
                null,
                new SearchOptions().setFilter(
                        partition.getPartitionFilter()
                )
        );

        return iterable.stream()
                .map(searchResult -> searchResult.getDocument(SearchDocument.class))
                .collect(Collectors.toList());
    }
}
