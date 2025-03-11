package io.github.jarol.azure.search.spark.connector.core.schema

import io.github.jarol.azure.search.spark.connector.core.DataTypeException
import org.apache.spark.sql.types._

/**
 * Set of utility methods for a [[DataType]]
 * @param input input data type
 */

class SparkTypeOperations(override protected val input: DataType)
  extends DataTypeOperations[DataType](input, "Spark") {

  override final def isString: Boolean = input.equals(DataTypes.StringType)

  override final def isNumeric: Boolean = {

    input match {
      case DataTypes.IntegerType | DataTypes.LongType | DataTypes.DoubleType => true
      case _ => false
    }
  }

  override def isBoolean: Boolean = input.equals(DataTypes.BooleanType)

  override def isDateTime: Boolean = {

    input match {
      case DataTypes.DateType | DataTypes.TimestampType => true
      case _ => false
    }
  }

  override def isCollection: Boolean = {

    input match {
      case _: ArrayType => true
      case _ => false
    }
  }

  /**
   * Return true for struct types
   * @return true for struct types
   */

  override def isComplex: Boolean = {

    input match {
      case _: StructType => true
      case _ => false
    }
  }

  override def safeCollectionInnerType: Option[DataType] = {

    input match {
      case ArrayType(elementType, _) => Some(elementType)
      case _ => None
    }
  }

  /**
   * Safely retrieve the type subfields
   * @return a non-empty collection of subfields, if this type is a struct
   */

  final def safeSubFields: Option[Seq[StructField]] = {

    input match {
      case StructType(fields) => Some(fields)
      case _ => None
    }
  }

  /**
   * Unsafely retrieve the type subfields
   * @throws DataTypeException if this type is not a struct
   * @return struct sub fields
   */

  @throws[DataTypeException]
  final def unsafeSubFields: Seq[StructField] = {

    safeSubFields match {
      case Some(value) => value
      case None => throw new DataTypeException(s"Could not retrieve subfield for $description $input")
    }
  }
}
