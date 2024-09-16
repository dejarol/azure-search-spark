package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.DataTypeException
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.input.{AtomicSparkInternalConverters, SparkInternalConverter}
import org.apache.spark.sql.types.{DataType, DataTypes}

object AtomicTypeConversionRules {

  /**
   * Inference rule for strings
   */

  private case object StringRule
    extends InferSchemaRule {
    override def sparkType(): DataType = DataTypes.StringType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.STRING
    override def converter(): SparkInternalConverter = AtomicSparkInternalConverters.StringConverter
  }

  /**
   * Inference rule for integers
   */

  private case object Int32Rule
    extends InferSchemaRule {
    override def sparkType(): DataType = DataTypes.IntegerType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.INT32
    override def converter(): SparkInternalConverter = AtomicSparkInternalConverters.Int32Converter
  }

  /**
   * Inference rule for longs
   */

  private case object Int64Rule
    extends InferSchemaRule {
    override def sparkType(): DataType = DataTypes.LongType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.INT64
    override def converter(): SparkInternalConverter = AtomicSparkInternalConverters.Int64Converter
  }

  /**
   * Inference rule for doubles
   */

  private case object DoubleRule
    extends InferSchemaRule {
    override def sparkType(): DataType = DataTypes.DoubleType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.DOUBLE
    override def converter(): SparkInternalConverter = AtomicSparkInternalConverters.DoubleConverter
  }

  /**
   * Inference rule for floats
   */

  private case object SingleRule
    extends InferSchemaRule {
    override def sparkType(): DataType = DataTypes.FloatType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.SINGLE
    override def converter(): SparkInternalConverter = AtomicSparkInternalConverters.SingleConverter
  }

  /**
   * Inference rule for boolean
   */

  private case object BooleanRule
    extends InferSchemaRule {
    override def sparkType(): DataType = DataTypes.BooleanType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.BOOLEAN
    override def converter(): SparkInternalConverter = AtomicSparkInternalConverters.BooleanConverter
  }

  /**
   * Inference for date times (timestamps)
   */

  private case object DateTimeToTimestampRule
    extends InferSchemaRule {
    override def sparkType(): DataType = DataTypes.TimestampType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.DATE_TIME_OFFSET
    override def converter(): SparkInternalConverter = AtomicSparkInternalConverters.DateTimeToTimestampConverter
  }

  /**
   * Schema conversion rule from date time to date
   */

  private case object DateTimeToDateRule
    extends SchemaConversionRule {
    override def sparkType(): DataType = DataTypes.DateType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.DATE_TIME_OFFSET
    override def converter(): SparkInternalConverter = AtomicSparkInternalConverters.DateTimeToDateConverter
  }

  /**
   * Schema conversion rule from date time to string
   */

  private case object DateTimeToStringRule
    extends SchemaConversionRule {
    override def sparkType(): DataType = DataTypes.StringType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.DATE_TIME_OFFSET
    override def converter(): SparkInternalConverter = AtomicSparkInternalConverters.StringConverter
  }

  private val ALL_RULES: Set[SearchSparkConversionRule] = Set(
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

  /**
   * Safely retrieve the inferred Spark dataType of a SearchField.
   *
   * It will return a non-empty Option for atomic fields (i.e. strings, numbers, boolean and dates)
   * for which an [[InferSchemaRule]] with same search type exists
   * @param `type` search field type
   * @return a non-empty Option if the field is atomic and there exists an infer schema rule
   */

  final def safeInferredSparkTypeOf(`type`: SearchFieldDataType): Option[DataType] = {

    ALL_RULES.collectFirst {
      case rule: InferSchemaRule if rule.acceptsSearchType(`type`) =>
        rule.sparkType()
    }
  }

  /**
   * Unsafely retrieve the inferred Spark type of Search type
   * @param `type` Search type
   * @throws DataTypeException if the Spark type could not be inferred
   * @return the inferred Spark type
   */

  @throws[DataTypeException]
  final def unsafeInferredSparkTypeOf(`type`: SearchFieldDataType): DataType = {

    safeInferredSparkTypeOf(`type`) match {
      case Some(value) => value
      case None => throw new DataTypeException(s"Could not find infer Spark type for SearchType ${`type`}")
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

  final def safeInferredSearchTypeOf(`type`: DataType): Option[SearchFieldDataType] = {

    ALL_RULES.collectFirst {
      case rule: InferSchemaRule if rule.acceptsSparkType(`type`) =>
        rule.searchType()
    }
  }

  /**
   * Unsafely retrieve the inferred Search type of a Spark type
   * @param `type` Search type
   * @throws DataTypeException if the Search type could not be inferred
   * @return the inferred Spark type
   */

  @throws[DataTypeException]
  final def unsafeInferredSearchTypeOf(`type`: DataType): SearchFieldDataType = {

    safeInferredSearchTypeOf(`type`) match {
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

    ALL_RULES.exists {
      rule => rule.useForSchemaConversion() &&
        !rule.useForSchemaInference() &&
        rule.acceptsTypes(sparkType, searchType)
    }
  }

  /**
   * Safely retrieve the converter (either from inference or conversion rules) from a Spark type to a Search type
   * @param sparkType Spark type
   * @param searchType Search type
   * @return an optional converter
   */

  final def safeConverterForTypes(sparkType: DataType, searchType: SearchFieldDataType): Option[SparkInternalConverter] = {

    // Converter from inference rules
    val maybeInferredConverter = ALL_RULES.collectFirst {
      case rule: InferSchemaRule
        if rule.acceptsTypes(sparkType, searchType) => rule.converter()
    }

    // Converter from conversion rules
    val maybeConversionConverter = ALL_RULES.collectFirst {
      case rule: SchemaConversionRule
        if rule.acceptsTypes(sparkType, searchType) => rule.converter()
    }

    // Inference rule takes precedence, if defined
    maybeInferredConverter
      .orElse(maybeConversionConverter)
  }
}
