package com.github.jarol.azure.search.spark.sql.connector;

import com.azure.search.documents.SearchClient;
import com.azure.search.documents.SearchDocument;
import com.azure.search.documents.indexes.models.IndexDocumentsBatch;
import com.azure.search.documents.models.IndexAction;
import com.azure.search.documents.models.IndexActionType;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.stream.Collectors;

public final class SearchTestUtils {

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
