package com.github.jarol.azure.search.spark.sql.connector.read.filter;

/**
 * Interface for implementing adapters from Spark pushed predicates to Azure Search OData expressions
 * (field references, literal value, filters, etc ...)
 * <br>
 * As Azure Search service adopts OData syntax for filtering, so each concrete instance should return
 * a OData-syntax compliant expression
 */

@FunctionalInterface
public interface V2ExpressionAdapter {

    /**
     * Get the corresponding OData expression
     * @return an OData-syntax compliant expression
     */

    String getODataExpression();
}
