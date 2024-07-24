package com.github.jarol.azure.search.spark.sql.connector.config;

import java.io.Serializable;

/**
 * Interface for Search read/write configurations
 */

public interface SearchConfig
        extends Serializable {

    String PREFIX = "spark.azure.search.";
    String READ_PREFIX = PREFIX + "read.";
    String WRITE_PREFIX = PREFIX + "write.";

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
