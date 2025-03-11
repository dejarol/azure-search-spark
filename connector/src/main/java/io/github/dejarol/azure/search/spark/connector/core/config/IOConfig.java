package io.github.dejarol.azure.search.spark.connector.core.config;

import java.io.Serializable;

/**
 * Interface for Search read/write configurations
 */

public interface IOConfig
        extends Serializable {

    String END_POINT_CONFIG = "endpoint";
    String API_KEY_CONFIG = "apiKey";
    String INDEX_CONFIG = "index";

    /**
     * Get the target Search endpoint
     * @return target Search endpoint
     */

    String getEndpoint();

    /**
     * Get the API key (for authentication)
     * @return the API key
     */

    String getAPIkey();

    /**
     * Get the name of the target index
     * @return target index name
     */

    String getIndex();
}
