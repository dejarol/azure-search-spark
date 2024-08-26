package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.AzureSparkException
import org.apache.spark.sql.types.{DataType, DataTypes}

object AtomicInferSchemaRules
  extends Set[InferSchemaRule] {

  private case object StringRule
    extends InferSchemaRule {
    override def sparkType(): DataType = DataTypes.StringType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.STRING
    override def converter(): SparkInternalConverter = AtomicTypeConverters.StringConverter
  }

  private case object Int32Rule
    extends InferSchemaRule {
    override def sparkType(): DataType = DataTypes.IntegerType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.INT32
    override def converter(): SparkInternalConverter = AtomicTypeConverters.Int32Converter
  }

  private case object Int64Rule
    extends InferSchemaRule {
    override def sparkType(): DataType = DataTypes.LongType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.INT64
    override def converter(): SparkInternalConverter = AtomicTypeConverters.Int64Converter
  }

  private case object DoubleRule
    extends InferSchemaRule {
    override def sparkType(): DataType = DataTypes.DoubleType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.DOUBLE
    override def converter(): SparkInternalConverter = AtomicTypeConverters.DoubleConverter
  }

  private case object SingleRule
    extends InferSchemaRule {
    override def sparkType(): DataType = DataTypes.FloatType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.SINGLE
    override def converter(): SparkInternalConverter = AtomicTypeConverters.SingleConverter
  }

  private case object BooleanRule
    extends InferSchemaRule {
    override def sparkType(): DataType = DataTypes.BooleanType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.BOOLEAN
    override def converter(): SparkInternalConverter = AtomicTypeConverters.BooleanConverter
  }

  private case object DateTimeToTimestampRule
    extends InferSchemaRule {
    override def sparkType(): DataType = DataTypes.TimestampType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.DATE_TIME_OFFSET
    override def converter(): SparkInternalConverter = AtomicTypeConverters.DateTimeToTimestampConverter
  }

  private lazy val ALL_INFERENCE_RULES: Set[InferSchemaRule] = Set(
    StringRule,
    Int32Rule,
    Int64Rule,
    DoubleRule,
    SingleRule,
    BooleanRule,
    DateTimeToTimestampRule
  )
  override def contains(elem: InferSchemaRule): Boolean = ALL_INFERENCE_RULES.contains(elem)

  override def +(elem: InferSchemaRule): Set[InferSchemaRule] = ALL_INFERENCE_RULES + elem

  override def -(elem: InferSchemaRule): Set[InferSchemaRule] = ALL_INFERENCE_RULES - elem

  override def iterator: Iterator[InferSchemaRule] = ALL_INFERENCE_RULES.iterator

  /**
   * Safely retrieve the inferred Spark dataType of a SearchField.
   *
   * It will return a non-empty Option for atomic fields (i.e. strings, numbers, boolean and dates)
   * for which an [[InferSchemaRule]] with same search type exists
   * @param `type` search field
   * @return a non-empty Option if the field is atomic and there exists an infer schema rule
   */

  final def safeInferredTypeOf(`type`: SearchFieldDataType): Option[DataType] = {

    ALL_INFERENCE_RULES.collectFirst {
      case rule if rule.searchType().equals(`type`) =>
        rule.sparkType()
    }
  }

  final def unsafeInferredTypeOf(`type`: SearchFieldDataType): DataType = {

    safeInferredTypeOf(`type`) match {
      case Some(value) => value
      case None => throw new AzureSparkException(s"Could not find equivalent atomic type for SearchType ${`type`}")
    }
  }
}
