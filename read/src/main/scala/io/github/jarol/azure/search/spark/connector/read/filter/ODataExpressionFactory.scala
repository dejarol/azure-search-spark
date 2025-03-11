package io.github.jarol.azure.search.spark.connector.read.filter

/**
 * Mix-in trait for objects that should act as factories for [[ODataExpression]](s),
 * defining a safe method for converting a given type instance into an OData expression
 * @tparam T type instance
 */

trait ODataExpressionFactory[T] {

  /**
   * Safely converts an expression into an OData expression
   * @param expression expression
   * @return a non-empty OData expression if the given expression is supported
   */

  def build(expression: T): Option[ODataExpression]

}