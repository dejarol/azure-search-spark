package com.github.jarol.azure.search.spark.sql.connector.models;

/**
 * Mix-in interface for documents to use for integration testing
 */

public interface ITDocument {

    /**
     * Gets this document's id
     * @return the document's id
     */

    String id();
}
