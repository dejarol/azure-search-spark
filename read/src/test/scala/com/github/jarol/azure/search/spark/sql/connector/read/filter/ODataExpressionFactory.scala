package com.github.jarol.azure.search.spark.sql.connector.read.filter

import org.apache.spark.sql.types.DataTypes
import org.apache.spark.unsafe.types.UTF8String

import java.time.LocalDate

/**
 * Trait to mix-in for creating [[ODataExpression]](s)
 */

trait ODataExpressionFactory {

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

    ODataExpressions.literal(
      DataTypes.StringType,
      UTF8String.fromString(value)
    )
  }

  /**
   * Create an integer literal
   * @param value literal value
   * @return an [[ODataExpression]] representing a numeric literal
   */

  protected final def createIntLiteral(value: Int): ODataExpression = {

    ODataExpressions.literal(
      DataTypes.IntegerType,
      value
    )
  }

  /**
   * Create a datetime literal
   * @param value literal value
   * @return an [[ODataExpression]] representing a datetime literal
   */

  protected final def createDateLiteral(value: LocalDate): ODataExpression = {

    ODataExpressions.literal(
      DataTypes.DateType,
      value.toEpochDay.toInt
    )
  }
}
