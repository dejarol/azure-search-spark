package com.github.jarol.azure.search.spark.sql.connector.read.partitioning;

import com.azure.search.documents.models.SearchOptions;

public class SimpleSearchPartition implements SearchPartition {

    @Override
    public SearchOptions getSearchOptions() {
        return new SearchOptions();
    }
}
