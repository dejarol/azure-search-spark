package io.github.jarol.azure.search.spark.connector.read.filter

import io.github.jarol.azure.search.spark.connector.core.Constants
import java.sql.Timestamp
import java.time.LocalDate

/**
 * Mix-in trait for creating [[ODataExpression]](s)
 */

trait ODataExpressionMixins {

  import ODataLiterables._

  /**
   * Create an expression for a top-level field
   * @param name field name
   * @return an expression for a top-level field
   */

  protected final def topLevelFieldReference(name: String): ODataExpression = ODataExpressions.fieldReference(Seq(name))

  /**
   * Create a string literal
   * @param value literal value
   * @return an [[ODataExpression]] representing a string literal
   */

  protected final def createStringLiteral(value: String): ODataExpression = {

    ODataExpressions.Literal[String](value)
  }

  /**
   * Create an integer literal
   * @param value literal value
   * @return an [[ODataExpression]] representing a numeric literal
   */

  protected final def createIntLiteral(value: Int): ODataExpression = {

    ODataExpressions.Literal[Integer](value)(numericLiterable)
  }

  /**
   * Create a datetime literal
   * @param value literal value
   * @return an [[ODataExpression]] representing a datetime literal
   */

  protected final def createDateLiteral(value: LocalDate): ODataExpression = {

    ODataExpressions.Literal[Timestamp](
      Timestamp.from(
        value.atStartOfDay(Constants.UTC_OFFSET).toInstant
      )
    )
  }
}
