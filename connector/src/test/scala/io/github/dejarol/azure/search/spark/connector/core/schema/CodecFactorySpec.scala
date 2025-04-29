package io.github.dejarol.azure.search.spark.connector.core.schema

import io.github.dejarol.azure.search.spark.connector.{BasicSpec, FieldFactory}
import org.scalatest.EitherValues
import org.scalatest.matchers.{BeMatcher, MatchResult}

import scala.language.implicitConversions

/**
 * Mix-in trait for testing subclasses of [[CodecFactory]]
 */

trait CodecFactorySpec
  extends BasicSpec
    with FieldFactory
      with EitherValues {

  import CodecFactorySpec._

  protected final val complex = new ComplexObjectErrorMatcher

  /**
   * Implicit conversion for creating an instance of [[ComplexObjectErrorOperations]] from an error
   * @param error input error
   * @return a utility class for dealing with error instances
   */

  protected final implicit def toComplexOperations(error: CodecError): ComplexObjectErrorOperations = {
    new ComplexObjectErrorOperations(error)
  }
}

object CodecFactorySpec {

  /**
   * Custom matcher for asserting that an instance of [[CodecError]] is a [[CodecErrors.ComplexObjectError]]
   */

  class ComplexObjectErrorMatcher
    extends BeMatcher[CodecError] {

    override def apply(left: CodecError): MatchResult = {
      MatchResult(
        left match {
          case _: CodecErrors.ComplexObjectError => true
          case _ => false
        },
        "this error is not complex",
        "this error is complex"
      )
    }
  }

  /**
   * Utility class for accessing the internal errors of a complex error
   * @param error codec error instance
   */

  class ComplexObjectErrorOperations(private val error: CodecError) {

    /**
     * Retrieves the internal errors of this error
     * @throws IllegalStateException if this error is not complex
     * @return the internal errors
     */

    @throws[IllegalStateException]
    def internal: Map[String, CodecError] = {
      error match {
        case c: CodecErrors.ComplexObjectError => c.internal
        case _ => throw new IllegalStateException(
          s"Cannot access the internal errors of a non-complex error. " +
            s"Please make sure to run the following assertion 'error shouldBe complex' before invoking this method"
        )
      }
    }
  }
}
