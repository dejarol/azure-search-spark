package com.github.jarol.azure.search.spark.sql.connector.schema

import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, StructType}

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

  final def isStruct: Boolean = {

    input match {
      case _: StructType => true
      case _ => false
    }
  }
}
