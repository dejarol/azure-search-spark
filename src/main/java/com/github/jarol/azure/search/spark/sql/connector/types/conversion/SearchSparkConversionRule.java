package com.github.jarol.azure.search.spark.sql.connector.types.conversion;

import com.azure.search.documents.indexes.models.SearchFieldDataType;
import org.apache.spark.sql.types.DataType;

public interface SearchSparkConversionRule {

    DataType sparkType();

    SearchFieldDataType searchType();

    boolean useForSchemaInference();
}
