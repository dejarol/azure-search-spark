package io.github.dejarol.azure.search.spark.connector.read.filter;

/**
 * Interface that translates Spark pushed predicates to Azure Search OData <code>\$filter</code> expressions
 * (field references, literal value, filters, etc ...)
 * <br>
 * As Azure Search service adopts OData syntax for filtering, so each concrete instance should return
 * an OData-syntax compliant expression
 */

public interface ODataExpression {

    /**
     * Get the corresponding OData expression
     * @return an OData-syntax compliant expression
     */

    String toUriLiteral();
}
