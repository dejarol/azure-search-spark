package io.github.dejarol.azure.search.spark.connector.core.schema

import com.azure.search.documents.indexes.models.SearchFieldDataType
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}
import org.json4s._

/**
 * Parent class for modeling codec errors
 * <p>
 * Subclasses should take care of implementing [[toJValue]], which should be responsible
 * for providing a JSON representation of the error
 */

sealed trait CodecError {

  /**
   * Gets the JSON representation of this error
   * @return JSON representation
   */

  def toJValue: JValue
}

object CodecErrors {

  private def descriptionForSparkType(dataType: DataType): String = {

    // When dealing with StructType, use its simple string representation
    dataType match {
      case s: StructType => s.simpleString
      case a: ArrayType => a.simpleString
      case _ => dataType.typeName
    }
  }

  /**
   * Error occurring when Spark and Search types are incompatible
   * @param sparkType Spark type
   * @param searchType Search type
   */

  private case class DTypesError(
                                  sparkType: DataType,
                                  searchType: SearchFieldDataType
                                ) extends CodecError {

    override def toJValue: JValue = {

      JString(
        s"Spark type ${descriptionForSparkType(sparkType)} is not compatible " +
          s"with Search type ${searchType.toString}"
      )
    }
  }

  /**
   * Error occurring when a field is missing
   */

  private case object MISSING_FIELD_ERROR
    extends CodecError {

    override def toJValue: JValue = {

      JString(
        "Field is missing"
      )
    }
  }

  /**
   * Error occurring during the encoding/decoding of a complex object.
   * <br>
   * It contains a map with keys being subfield names and values being errors for such field
   * @param internal collection of subfield errors
   */

  private case class ComplexObjectError(internal: Map[String, CodecError])
    extends CodecError {

    override def toJValue: JValue = {

      JObject(
        internal.mapValues(_.toJValue).toList
      )
    }
  }

  /**
   * Error occurring when a datatype is not suitable for GeoPoint
   * @param dataType datatype
   */

  private case class NotSuitableForGeoPoint(dataType: DataType)
    extends CodecError {

    override def toJValue: JValue = {

      JString(
        s"Datatype ${descriptionForSparkType(dataType)} is not suitable for GeoPoint"
      )
    }
  }

  /**
   * Creates a [[CodecError]] for a field with incompatible Spark and Search types
   * @param sparkType Spark type
   * @param searchType Search type
   * @return a [[CodecError]]
   */

  def forIncompatibleTypes(
                            sparkType: DataType,
                            searchType: SearchFieldDataType
                          ): CodecError = {

    DTypesError(sparkType, searchType)
  }

  /**
   * Creates a [[CodecError]] for a missing field
   * @return a [[CodecError]]
   */

  def forMissingField(): CodecError = MISSING_FIELD_ERROR

  /**
   * Creates a [[CodecError]] for a complex object
   * @param subErrors errors related to the subfields of the complex object
   * @return a [[CodecError]]
   */

  def forComplexObject(subErrors: Map[String, CodecError]): CodecError = ComplexObjectError(subErrors)

  /**
   * Creates a [[CodecError]] for a datatype which is not suitable for GeoPoint
   * @param dataType Spark type
   * @return a [[CodecError]]
   */

  def notSuitableForGeoPoint(dataType: DataType): CodecError = NotSuitableForGeoPoint(dataType)
}
