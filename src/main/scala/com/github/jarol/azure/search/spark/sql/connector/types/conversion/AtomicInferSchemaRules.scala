package com.github.jarol.azure.search.spark.sql.connector.types.conversion

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.AzureSparkException
import org.apache.spark.sql.types.{DataType, DataTypes}

object AtomicInferSchemaRules
  extends RuleSet {

  private case object StringRule
    extends InferSchemaRule {
    override def sparkType(): DataType = DataTypes.StringType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.STRING
  }

  private case object Int32Rule
    extends InferSchemaRule {
    override def sparkType(): DataType = DataTypes.IntegerType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.INT32
  }

  private case object Int64Rule
    extends InferSchemaRule {
    override def sparkType(): DataType = DataTypes.LongType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.INT64
  }

  private case object DoubleRule
    extends InferSchemaRule {
    override def sparkType(): DataType = DataTypes.DoubleType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.DOUBLE
  }

  private case object SingleRule
    extends InferSchemaRule {
    override def sparkType(): DataType = DataTypes.FloatType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.SINGLE
  }

  private case object BooleanRule
    extends InferSchemaRule {
    override def sparkType(): DataType = DataTypes.BooleanType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.BOOLEAN
  }

  private case object DateTimeToTimestampRule
    extends InferSchemaRule {
    override def sparkType(): DataType = DataTypes.TimestampType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.DATE_TIME_OFFSET
  }

  private lazy val ALL_INFERENCE_RULES: Set[SearchSparkConversionRule] = Set(
    StringRule,
    Int32Rule,
    Int64Rule,
    DoubleRule,
    SingleRule,
    BooleanRule,
    DateTimeToTimestampRule
  )

  /**
   * Return all the defined atomic rules
   * @return all atomic rules
   */

  final def allRules(): Set[SearchSparkConversionRule] = ALL_INFERENCE_RULES

  /**
   * Safely retrieve the predefined [[SearchSparkConversionRule]] for inferring an atomic search type
   * @param `type` search type
   * @return an empty Option if the search type is not atomic, a defined Option otherwise
   */

  final def safeRuleForType(`type`: SearchFieldDataType): Option[SearchSparkConversionRule] = {

    collectFirst {
      rule => rule.searchType().equals(`type`)
    }
  }

  /**
   * Evaluate if an inference rule for a search type exists
   * @param `type` search type
   * @return true if there exists an inference rule for given search type atomic
   */

  final def existsRuleForType(`type`: SearchFieldDataType): Boolean = safeRuleForType(`type`).isDefined

  /**
   * Unsafely get the inference rule for a search type
   * @param `type` search type
   * @throws AzureSparkException if given search type is not atomic, or an inference rule could not be found
   * @return the inference rule related to given search type
   */

  @throws[AzureSparkException]
  final def unsafeRuleForType(`type`: SearchFieldDataType): SearchSparkConversionRule = {

    safeRuleForType(`type`) match {
      case Some(value) => value
      case None => throw new AzureSparkException(s"Could not get a conversion rule for type ${`type`}")
    }
  }
}
