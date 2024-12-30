package com.github.jarol.azure.search.spark.sql.connector.read.filter;

/**
 * Interface for implementing adapters from Spark pushed predicates to Azure Search filters.
 * <br>
 * As Azure Search service adopts OData syntax for filtering, each concrete instance should return
 * a valid OData filter
 */

@FunctionalInterface
public interface V2PushDownFilterAdapter {

    /**
     * Get the corresponding OData filter
     * @return OData filter string
     */

    String getODataFilter();
}
