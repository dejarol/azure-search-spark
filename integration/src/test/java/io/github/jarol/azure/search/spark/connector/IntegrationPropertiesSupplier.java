package io.github.jarol.azure.search.spark.connector;

/**
 * Mix-in interface for implementing classes that should retrieve properties
 * and values required by integration tests, like Azure Search endpoint, api key, etc ...
 */

public interface IntegrationPropertiesSupplier {

    /**
     * Get the Azure Search endpoint
     * @return Azure search endpoint
     */

    String endPoint();

    /**
     * Get the Azure Search api key
     * @return Azure Search api key
     */

    String apiKey();
}
