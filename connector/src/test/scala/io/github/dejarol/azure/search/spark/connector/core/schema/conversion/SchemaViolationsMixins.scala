package io.github.dejarol.azure.search.spark.connector.core.schema.conversion

import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters

/**
 * Mix-in trait for dealing with instances of [[SchemaViolation]]
 */

trait SchemaViolationsMixins {

  /**
   * Returns true if this instance is a [[SchemaViolations.IncompatibleType]]
   * @param v violation
   * @return true for incompatible type violations
   */

  def isForIncompatibleType(v: SchemaViolation): Boolean = v.isInstanceOf[SchemaViolations.IncompatibleType]

  /**
   * Returns true if this instance refers to a complex field violation
   * @param v complex field violation
   * @return true for complex field violations
   */

  def isForComplexField(v: SchemaViolation): Boolean = v.isInstanceOf[SchemaViolations.ComplexFieldViolation]

  /**
   * Returns true if this instance refers to an array field violation
   * @param v array violation
   * @return true for array field violations
   */

  def isForArrayField(v: SchemaViolation): Boolean = v.isInstanceOf[SchemaViolations.ArrayViolation]

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
