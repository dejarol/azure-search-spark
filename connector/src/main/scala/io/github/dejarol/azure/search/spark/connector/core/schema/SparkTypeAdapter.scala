package io.github.dejarol.azure.search.spark.connector.core.schema

import io.github.dejarol.azure.search.spark.connector.core.EntityDescription
import org.apache.spark.sql.types._

/**
 * Set of utility methods for dealing with a [[org.apache.spark.sql.types.DataType]]
 * @param input input data type
 */

class SparkTypeAdapter(private val input: DataType)
  extends DataTypeAdapter[DataType]
    with SubFieldsSupplier[StructField]
    with EntityDescription {

  override def description: String = f"Spark type ${input.typeName}"

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

  override def safeSubFields: Option[Seq[StructField]] = {

    input match {
      case StructType(fields) => Some(fields)
      case _ => None
    }
  }
}
