package com.github.jarol.azure.search.spark.sql.connector.core.schema

import com.github.jarol.azure.search.spark.sql.connector.core.DataTypeException
import org.apache.spark.sql.types._

/**
 * Set of utility methods for a [[DataType]]
 * @param input input data type
 */

class SparkTypeOperations(override protected val input: DataType)
  extends DataTypeOperations[DataType](input, "Spark") {

  override final def isString: Boolean = input.equals(DataTypes.StringType)

  override final def isNumeric: Boolean = {

    Seq(
      DataTypes.IntegerType,
      DataTypes.LongType,
      DataTypes.DoubleType,
      DataTypes.FloatType
    ).exists {
      _.equals(input)
    }
  }

  override def isBoolean: Boolean = input.equals(DataTypes.BooleanType)

  override def isDateTime: Boolean = {

    input.equals(DataTypes.DateType) ||
      input.equals(DataTypes.TimestampType)
  }

  override def isCollection: Boolean = {

    input match {
      case _: ArrayType => true
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
   * Evaluate if this type is a struct type
   * @return true for struct types
   */

  final def isStruct: Boolean = {

    input match {
      case _: StructType => true
      case _ => false
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
