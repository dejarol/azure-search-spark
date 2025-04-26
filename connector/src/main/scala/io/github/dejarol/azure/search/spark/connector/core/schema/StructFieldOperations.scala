package io.github.dejarol.azure.search.spark.connector.core.schema

import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, StructField, StructType}

/**
 * Operational class for managing [[org.apache.spark.sql.types.StructField]](s)
 * @param structField a Spark field
 */

case class StructFieldOperations(private val structField: StructField)
   extends FieldOperations[DataType, StructField]
     with FieldDescriptor {

  private val dataType: DataType = structField.dataType

  override def isString: Boolean = dataType.equals(DataTypes.StringType)

  override def isNumeric: Boolean = {

    dataType match {
      case DataTypes.IntegerType | DataTypes.LongType | DataTypes.DoubleType => true
      case _ => false
    }
  }

  override def isBoolean: Boolean = dataType.equals(DataTypes.BooleanType)

  override def isDateTime: Boolean = {

    dataType match {
      case DataTypes.DateType | DataTypes.TimestampType => true
      case _ => false
    }
  }

  override def isCollection: Boolean = {

    dataType match {
      case _: ArrayType => true
      case _ => false
    }
  }

  override def isComplex: Boolean = {

    dataType match {
      case _: StructType => true
      case _ => false
    }
  }

  override def safeCollectionInnerType: Option[DataType] = {

    dataType match {
      case ArrayType(elementType, _) => Some(elementType)
      case _ => None
    }
  }

  override def safeSubFields: Option[Seq[StructField]] = {

    dataType match {
      case StructType(fields) => Some(fields)
      case _ => None
    }
  }

  override def name(): String = structField.name

  override def `type`(): String = "Spark"

  override def dataTypeDescription(): String = dataType.typeName
}
