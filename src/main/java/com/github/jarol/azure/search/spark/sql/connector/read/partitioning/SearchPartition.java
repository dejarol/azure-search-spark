package com.github.jarol.azure.search.spark.sql.connector.read.partitioning;

import com.azure.search.documents.models.SearchOptions;
import org.apache.spark.sql.connector.read.InputPartition;

public interface SearchPartition extends InputPartition {

    SearchOptions getSearchOptions();
}
