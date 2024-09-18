package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.DataTypeException
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.input.{AtomicSparkInternalConverters, SparkInternalConverter}
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.output.{AtomicSearchConverters, SearchPropertyConverter}
import org.apache.spark.sql.types.{DataType, DataTypes}

object AtomicTypeConversionRules {

  /**
   * Inference rule for strings
   */

  private case object StringRule
    extends InferSchemaRule {
    override def sparkType(): DataType = DataTypes.StringType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.STRING
    override def sparkConverter(): SparkInternalConverter = AtomicSparkInternalConverters.StringConverter
    override def searchConverter(): SearchPropertyConverter = AtomicSearchConverters.StringConverter
  }

  /**
   * Inference rule for integers
   */

  private case object Int32Rule
    extends InferSchemaRule {
    override def sparkType(): DataType = DataTypes.IntegerType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.INT32
    override def sparkConverter(): SparkInternalConverter = AtomicSparkInternalConverters.Int32Converter
    override def searchConverter(): SearchPropertyConverter = AtomicSearchConverters.Int32Converter
  }

  /**
   * Inference rule for longs
   */

  private case object Int64Rule
    extends InferSchemaRule {
    override def sparkType(): DataType = DataTypes.LongType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.INT64
    override def sparkConverter(): SparkInternalConverter = AtomicSparkInternalConverters.Int64Converter
    override def searchConverter(): SearchPropertyConverter = AtomicSearchConverters.Int64Converter
  }

  /**
   * Inference rule for doubles
   */

  private case object DoubleRule
    extends InferSchemaRule {
    override def sparkType(): DataType = DataTypes.DoubleType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.DOUBLE
    override def sparkConverter(): SparkInternalConverter = AtomicSparkInternalConverters.DoubleConverter
    override def searchConverter(): SearchPropertyConverter = AtomicSearchConverters.DoubleConverter
  }

  /**
   * Inference rule for floats
   */

  private case object SingleRule
    extends InferSchemaRule {
    override def sparkType(): DataType = DataTypes.FloatType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.SINGLE
    override def sparkConverter(): SparkInternalConverter = AtomicSparkInternalConverters.SingleConverter
    override def searchConverter(): SearchPropertyConverter = AtomicSearchConverters.SingleConverter
  }

  /**
   * Inference rule for boolean
   */

  private case object BooleanRule
    extends InferSchemaRule {
    override def sparkType(): DataType = DataTypes.BooleanType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.BOOLEAN
    override def sparkConverter(): SparkInternalConverter = AtomicSparkInternalConverters.BooleanConverter
    override def searchConverter(): SearchPropertyConverter = AtomicSearchConverters.BooleanConverter
  }

  /**
   * Inference for date times (timestamps)
   */

  private case object DateTimeToTimestampRule
    extends InferSchemaRule {
    override def sparkType(): DataType = DataTypes.TimestampType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.DATE_TIME_OFFSET
    override def sparkConverter(): SparkInternalConverter = AtomicSparkInternalConverters.DateTimeToTimestampConverter
    override def searchConverter(): SearchPropertyConverter = AtomicSearchConverters.TimestampToDatetimeConverter
  }

  /**
   * Schema conversion rule from date time to date
   */

  private case object DateTimeToDateRule
    extends SchemaConversionRule {
    override def sparkType(): DataType = DataTypes.DateType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.DATE_TIME_OFFSET
    override def sparkConverter(): SparkInternalConverter = AtomicSparkInternalConverters.DateTimeToDateConverter
    override def searchConverter(): SearchPropertyConverter = AtomicSearchConverters.DateToDatetimeConverter
  }

  /**
   * Schema conversion rule from date time to string
   */

  private case object DateTimeToStringRule
    extends SchemaConversionRule {
    override def sparkType(): DataType = DataTypes.StringType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.DATE_TIME_OFFSET
    override def sparkConverter(): SparkInternalConverter = AtomicSparkInternalConverters.StringConverter
    override def searchConverter(): SearchPropertyConverter = AtomicSearchConverters.StringConverter
  }

  private lazy val ALL_RULES: Set[SearchSparkConversionRule] = Set(
    StringRule,
    Int32Rule,
    Int64Rule,
    DoubleRule,
    SingleRule,
    BooleanRule,
    DateTimeToTimestampRule,
    DateTimeToDateRule,
    DateTimeToStringRule
  )

  private lazy val INFERENCE_RULES: Set[InferSchemaRule] = ALL_RULES.collect {
    case i: InferSchemaRule => i
  }

  private lazy val CONVERSION_RULES: Set[SchemaConversionRule] = ALL_RULES.collect {
    case s: SchemaConversionRule => s
  }

  /**
   * Collect the first rule that fits with a partial function
   * <br>
   * Inference rules will be checked first, and conversion later
   * @param partialF partial function to match
   * @tparam A output type of the partial function
   * @return an optional instance from matching rule
   */

  private def collectFirstInferenceOrConversionRule[A](partialF: PartialFunction[SearchSparkConversionRule, A]): Option[A] = {

    INFERENCE_RULES.collectFirst(partialF)
      .orElse(CONVERSION_RULES.collectFirst(partialF))
  }

  /**
   * Safely retrieve the inferred Spark dataType of a SearchField.
   *
   * It will return a non-empty Option for atomic fields (i.e. strings, numbers, boolean and dates)
   * for which an [[InferSchemaRule]] with same search type exists
   * @param `type` search field type
   * @return a non-empty Option if the field is atomic and there exists an infer schema rule
   */

  final def safeInferredTypeOf(`type`: SearchFieldDataType): Option[DataType] = {

    INFERENCE_RULES.collectFirst {
      case rule if rule.acceptsSearchType(`type`) =>
        rule.sparkType()
    }
  }

  /**
   * Safely retrieve the inferred Search dataType of a Spark type.
   *
   * It will return a non-empty Option for atomic fields (i.e. strings, numbers, boolean and dates)
   * for which an [[InferSchemaRule]] with same search type exists
   * @param `type` Spark type
   * @return a non-empty Option if the field is atomic and there exists an infer schema rule
   */

  final def safeInferredTypeOf(`type`: DataType): Option[SearchFieldDataType] = {

    collectFirstInferenceOrConversionRule {
      case rule if rule.acceptsSparkType(`type`) =>
        rule.searchType()
    }
  }

  /**
   * Unsafely retrieve the inferred Spark type of Search type
   * @param `type` Search type
   * @throws DataTypeException if the Spark type could not be inferred
   * @return the inferred Spark type
   */

  @throws[DataTypeException]
  final def unsafeInferredTypeOf(`type`: SearchFieldDataType): DataType = {

    safeInferredTypeOf(`type`) match {
      case Some(value) => value
      case None => throw new DataTypeException(s"Could not find infer Spark type for SearchType ${`type`}")
    }
  }

  /**
   * Unsafely retrieve the inferred Search type of a Spark type
   * @param `type` Search type
   * @throws DataTypeException if the Search type could not be inferred
   * @return the inferred Spark type
   */

  @throws[DataTypeException]
  final def unsafeInferredTypeOf(`type`: DataType): SearchFieldDataType = {

    safeInferredTypeOf(`type`) match {
      case Some(value) => value
      case None => throw new DataTypeException(s"Could not find infer Search type for Spark type ${`type`}")
    }
  }

  /**
   * Evaluate if a conversion rule between a Spark type and a Search type exists
   * @param sparkType Spark type
   * @param searchType Search type
   * @return true for existing conversion rule
   */

  final def existsConversionRuleFor(sparkType: DataType, searchType: SearchFieldDataType): Boolean = {

    CONVERSION_RULES.exists {
      _.acceptsTypes(sparkType, searchType)
    }
  }

  /**
   * Safely retrieve the converter (either from inference or conversion rules) from a Search type to a Spark type
   * @param sparkType Spark type
   * @param searchType Search type
   * @return an optional converter
   */

  final def safeSparkConverterForTypes(sparkType: DataType, searchType: SearchFieldDataType): Option[SparkInternalConverter] = {

    collectFirstInferenceOrConversionRule {
      case rule if rule.acceptsTypes(sparkType, searchType) =>
        rule.sparkConverter()
    }
  }

  /**
   * Safely retrieve the Search converter (either from inference or conversion rules) from a Spark type to a Search type
   * @param sparkType Spark type
   * @param searchType Search type
   * @return an optional converter
   */

  final def safeSearchConverterForTypes(sparkType: DataType, searchType: SearchFieldDataType): Option[SearchPropertyConverter] = {

    collectFirstInferenceOrConversionRule {
      case rule if rule.acceptsTypes(sparkType, searchType) =>
        rule.searchConverter()
    }
  }
}
