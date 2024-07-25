package com.github.jarol.azure.search.spark.sql.connector.read.partitioning;

import com.azure.search.documents.models.SearchOptions;

public interface SearchPartition {

    SearchOptions getSearchOptions();
}
