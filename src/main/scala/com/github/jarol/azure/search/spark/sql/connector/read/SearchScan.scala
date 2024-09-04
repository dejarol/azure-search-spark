package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig
import com.github.jarol.azure.search.spark.sql.connector.schema._
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion._
import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

/**
 * Scan for Search dataSource
 * @param schema input schema (either inferred or user-defined)
 * @param readConfig read configuration
 * @param converters map with keys being field names and values being converters to use for extracting document values
 */

class SearchScan private (private val schema: StructType,
                          private val readConfig: ReadConfig,
                          private val converters: Map[String, SparkInternalConverter])
  extends Scan {

  override def readSchema(): StructType = schema

  override def toBatch: Batch = new SearchBatch(readConfig, converters)
}

object SearchScan {

  def safeApply(schema: StructType,
                searchFields: Seq[SearchField],
                readConfig: ReadConfig): Either[String, SearchScan] = {

    // Zip each StructField to the namesake Search field
    val indexName: String = readConfig.getIndex
    val sparkFieldsAndMaybeSearchFields: Map[StructField, Option[SearchField]] = schema.map {
      structField => (
        structField,
        searchFields.collectFirst {
          case searchField if searchField.sameNameOf(structField) => searchField
        })
    }.toMap

    // Retrieve the set of schema fields that do not exist on Search index
    val nonExistingSchemaFields: Seq[String] = sparkFieldsAndMaybeSearchFields.collect {
      case (k, None) => k.name
    }.toSeq

    if (nonExistingSchemaFields.nonEmpty) {
      leftMessageForNonExistingFields(nonExistingSchemaFields, indexName)
    } else {

      // Collect existing schema fields (which should be all due to previous evaluation)
      // and safely compute the conversion rule
      val sparkFieldsSearchFieldsAndMaybeConversionRule = sparkFieldsAndMaybeSearchFields.collect {
        case (k, Some(v)) => (k, v, safeConversionRuleFor(k, v))
      }

      // Collect the existing fields that have a data type incompatibility
      val sparkFieldsWithNonCompatibleSearchType: Map[StructField, SearchField] = sparkFieldsSearchFieldsAndMaybeConversionRule.collect {
        case (v1, v2, None) => (v1, v2)
      }.toMap

      if (sparkFieldsWithNonCompatibleSearchType.nonEmpty) {
        leftMessageForNonCompatibleFields(sparkFieldsWithNonCompatibleSearchType)
      } else {
        Right(
          new SearchScan(schema, readConfig, sparkFieldsSearchFieldsAndMaybeConversionRule.collect {
            case (v1, _, Some(rule)) => (v1.name, rule.converter())
          }.toMap)
        )
      }
    }
  }

  /**
   * Create a Left that will contain a message reporting non-existing schema fields
   * @param fields non-existing schema fields
   * @param index index name
   * @return a Left instance
   */

  private def leftMessageForNonExistingFields(fields: Traversable[String], index: String): Either[String, SearchScan] = {

    val size = fields.size
    val fieldNames = fields.mkString("(", ", ", ")")
    Left(s"$size schema fields ($fieldNames) do not exist on index $index")
  }

  /**
   * Create a Left that will contain a message reporting schema fields with incompatible datatypes
   * @param fields map with keys being Spark fields and values being namesake Search fields
   * @return a Left instance
   */

  private def leftMessageForNonCompatibleFields(fields: Map[StructField, SearchField]): Either[String, SearchScan] = {

    val size = fields.size
    val fieldNames = fields.map {
      case (k, v) => s"${k.name} (${k.dataType.typeName}, not compatible with ${v.getType})"
    }.mkString("(", ", ", ")")

    Left(s"$size schema fields have incompatible datatypes $fieldNames")
  }

  protected[read] def safeConversionRuleFor(structField: StructField,
                                            searchField: SearchField): Option[SearchSparkConversionRule] = {

    val (sparkType, searchFieldType) = (structField.dataType, searchField.getType)
    if (!searchField.sameNameOf(structField)) {
      None
    } else {

      if (searchFieldType.isAtomic) {
        AtomicInferSchemaRules.safeRuleForTypes(
          sparkType,
          searchFieldType
        ).orElse(
          AtomicSchemaConversionRules.safeRuleForTypes(
            sparkType,
            searchFieldType
          )
        )
      } else if (searchFieldType.isCollection) {

        val searchInnerType = searchFieldType.unsafelyExtractCollectionType
        sparkType match {
          case ArrayType(sparkInternalType, _) => safeConversionRuleFor(
            StructField("array", sparkInternalType),
            new SearchField("array", searchInnerType)
          ).map {
            rule => ArrayConversionRule(
              sparkInternalType,
              searchInnerType,
              rule.converter()
            )
          }
          case _ => None
        }
      } else if (searchFieldType.isComplex) {

        sparkType match {
          case StructType(sparkSubFields) =>

            val searchSubFields = JavaScalaConverters.listToSeq(searchField.getFields)
            val converters = sparkSubFields.map {
              spField => (
                spField,
                searchSubFields.collectFirst {
                  case seField if seField.sameNameOf(spField) => safeConversionRuleFor(spField, seField)
                }
              )
            }.collect {
              case (k, Some(Some(value))) => (k, value)
            }.toMap

            if (sparkSubFields.map(_.name).diff(converters.keySet.toSeq).nonEmpty) {
              None
            } else {
              Some(
                ComplexConversionRule(
                  sparkSubFields,
                  converters
                )
              )
            }
          case _ => None
        }
      } else if (searchFieldType.isGeoPoint) {
        Some(GeoPointRule)
      } else {
        None
      }
    }
  }
}
