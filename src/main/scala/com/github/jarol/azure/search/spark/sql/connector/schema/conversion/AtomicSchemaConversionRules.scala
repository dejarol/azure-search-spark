package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import com.azure.search.documents.indexes.models.SearchFieldDataType
import org.apache.spark.sql.types.{DataType, DataTypes}

object AtomicSchemaConversionRules
  extends SearchSparkConversionRuleSet {

  private case object DateTimeToDateRule
    extends SchemaConversionRule {

    override def sparkType(): DataType = DataTypes.DateType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.DATE_TIME_OFFSET
    override def converter(): SparkInternalConverter = AtomicTypeConverters.DateTimeToDateConverter
  }

  private case object DateTimeToStringRule
    extends SchemaConversionRule {

    override def sparkType(): DataType = DataTypes.StringType
    override def searchType(): SearchFieldDataType = SearchFieldDataType.DATE_TIME_OFFSET
    override def converter(): SparkInternalConverter = AtomicTypeConverters.StringConverter
  }

  override protected val ALL_RULES: Set[SearchSparkConversionRule] = Set(
    DateTimeToDateRule,
    DateTimeToStringRule
  )

  final def existsRuleFor(sparkType: DataType, searchType: SearchFieldDataType): Boolean = safeRuleForTypes(sparkType, searchType).isDefined
}
