package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion

import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters

/**
 * Mix-in trait for dealing with instances of [[SchemaViolation]]
 */

trait SchemaViolationsUtils {

  /**
   * Evaluate if a schema violation instance is an instance for a given subclass
   * @param v violation
   * @tparam T subclass type
   * @return true if given violation is an instance of given subclass type
   */

  protected final def isOfType[T <: SchemaViolation](v: SchemaViolation): Boolean = v.isInstanceOf[T]

  /**
   * Returns true if this instance is a [[SchemaViolations.IncompatibleType]]
   * @param v violation
   * @return true for incompatible type violations
   */

  def isForIncompatibleType(v: SchemaViolation): Boolean = isOfType[SchemaViolations.IncompatibleType](v)

  /**
   * Returns true if this instance refers to a complex field violation
   * @param v complex field violation
   * @return true for complex field violations
   */

  def isForComplexField(v: SchemaViolation): Boolean = isOfType[SchemaViolations.ComplexFieldViolation](v)

  /**
   * Returns true if this instance refers to an array field violation
   * @param v array violation
   * @return true for array field violations
   */

  def isForArrayField(v: SchemaViolation): Boolean = isOfType[SchemaViolations.ArrayViolation](v)

  /**
   * Maybe get subfield violations (defined if given a [[SchemaViolations.ComplexFieldViolation]])
   * @param v violation
   * @return optional sub-field violations
   */

  def maybeSubFieldViolations(v: SchemaViolation): Option[Seq[SchemaViolation]] = {

    v match {
      case c: SchemaViolations.ComplexFieldViolation => Some(
        JavaScalaConverters.listToSeq(
          c.getSubFieldViolations
        )
      )
      case _ => None
    }
  }

  /**
   * Maybe get the sub-field violation from an array field violation
   * @param v violation
   * @return optional subfield violation
   */

  def maybeSubViolation(v: SchemaViolation): Option[SchemaViolation] = {

    v match {
      case a: SchemaViolations.ArrayViolation => Some(a.getSubtypeViolation)
      case _ => None
    }
  }
}
