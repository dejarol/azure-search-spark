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

    // Retrieve the set of schema fields that do not exist on Search index
    val missingSchemaFields: Seq[String] = getMissingSchemaFields(schema, searchFields)
    if (missingSchemaFields.nonEmpty) {
      leftMessageForMissingFields(missingSchemaFields, readConfig.getIndex)
    } else {

      // Match each Spark field with its namesake Search field
      // and then compute the set of schema fields for which a conversion rule cannot be found
      val sparkAndSearchFields: Map[StructField, SearchField] = matchSchemaFieldsWithNamesakeSearchFields(schema, searchFields)
      val convertersMap: Map[String, SparkInternalConverter] = getConvertersMap(sparkAndSearchFields)
      val schemaFieldsWithoutConversionRule: Map[StructField, SearchField] = sparkAndSearchFields.filterNot {
        case (k, _) => convertersMap.exists {
          case (colName, _) => k.name.equalsIgnoreCase(colName)
        }
      }

      // If all schema fields have a conversion rule, create the Scan
      if (schemaFieldsWithoutConversionRule.nonEmpty) {
        leftMessageForNonCompatibleFields(schemaFieldsWithoutConversionRule)
      } else {
        Right(
          new SearchScan(
            schema,
            readConfig,
            convertersMap
          )
        )
      }
    }
  }

  private def getMissingSchemaFields(schema: Seq[StructField], searchFields: Seq[SearchField]): Seq[String] = {

    schema.collect {
      case spField if !searchFields.exists {
        seField => seField.sameNameOf(spField)
      } => spField.name
    }
  }

  /**
   * Create a Left that will contain a message reporting non-existing schema fields
   * @param fields non-existing schema fields
   * @param index index name
   * @return a Left instance
   */

  private def leftMessageForMissingFields(fields: Traversable[String], index: String): Either[String, SearchScan] = {

    val size = fields.size
    val fieldNames = fields.mkString("(", ", ", ")")
    Left(s"$size schema fields ($fieldNames) do not exist on index $index")
  }

  private def matchSchemaFieldsWithNamesakeSearchFields(schema: Seq[StructField], searchFields: Seq[SearchField]): Map[StructField, SearchField] = {

    schema.map {
      spField => (
        spField,
        searchFields.collectFirst {
          case seField if seField.sameNameOf(spField) => seField
        }
      )
    }.collect {
      case (k, Some(v)) => (k, v)
    }.toMap
  }

  private def getConvertersMap(fields: Map[StructField, SearchField]): Map[String, SparkInternalConverter] = {

    fields.map {
      case (k, v) => (k, safeConversionRuleFor(k, v))
    }.collect {
      case (k, Some(rule)) => (k.name, rule.converter())
    }
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

      // For atomic types, there should exist either an inference rule or a conversion rule
      if (searchFieldType.isAtomic) {
        AtomicInferSchemaRules.safeRuleForTypes(sparkType, searchFieldType)
          .orElse(AtomicSchemaConversionRules.safeRuleForTypes(sparkType, searchFieldType)
          )
      } else if (searchFieldType.isCollection) {

        // Evaluate rule for the inner type
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

        // Build a rule that wraps subField conversion rules
        sparkType match {
          case StructType(sparkSubFields) =>

            val convertersMap: Map[String, SparkInternalConverter] = getConvertersMap(
              matchSchemaFieldsWithNamesakeSearchFields(
                sparkSubFields,
                JavaScalaConverters.listToSeq(searchField.getFields)
              )
            )

            val forAllSubFieldsExistAConverter = sparkSubFields.forall {
              spField => convertersMap.exists {
                case (k, _) => spField.name.equalsIgnoreCase(k)
              }
            }
            if (forAllSubFieldsExistAConverter) {
              Some(ComplexConversionRule(sparkSubFields, convertersMap))
            } else {
              None
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
