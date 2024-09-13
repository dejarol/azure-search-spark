package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig
import com.github.jarol.azure.search.spark.sql.connector.schema.SchemaUtils
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.input.{SparkInternalConverter, SparkInternalConverters}
import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.{StructField, StructType}

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

  /**
   * Safely create a Scan instance. The Scan will be build only if
   * Spark schema and Search schema are compatible
   * @param schema schema (either inferred or used-defined)
   * @param searchFields Search index fields
   * @param readConfig read configuration
   * @return either a string reporting the incompatibility among the schemas or the scan instance
   */

  def safeApply(schema: StructType,
                searchFields: Seq[SearchField],
                readConfig: ReadConfig): Either[String, SearchScan] = {

    // Retrieve the set of schema fields that do not exist on Search index
    if (!SchemaUtils.allSchemaFieldsExist(schema, searchFields)) {
      leftMessageForMissingFields(
        SchemaUtils.getMissingSchemaFields(schema, searchFields),
        readConfig.getIndex
      )
    } else {

      // Match each Spark field with its namesake Search field
      // and then compute the set of schema fields for which a conversion rule cannot be found
      val sparkAndNamesakeSearchFields: Map[StructField, SearchField] = SchemaUtils.matchNamesakeFields(schema, searchFields)
      val allPairsAreCompatible: Boolean = sparkAndNamesakeSearchFields.forall {
        case (k, v) => SchemaUtils.areCompatibleFields(k, v)
      }

      if (!allPairsAreCompatible) {
        val nonCompatibleFields = sparkAndNamesakeSearchFields.filterNot {
          case (k, v) => SchemaUtils.areCompatibleFields(k, v)
        }
        leftMessageForNonCompatibleFields(nonCompatibleFields)
      } else {

        // Retrieve a converter for each schema field
        val converters: Map[String, SparkInternalConverter] = sparkAndNamesakeSearchFields.map {
          case (k, v) => (k.name, SparkInternalConverters.safeConverterFor(k, v))
        }.collect {
          case (k, Some(v)) => (k, v)
        }

        // Detect those schema fields for which a converter could not be found
        // If any, better to stop the Scan initialization
        val schemaFieldsWithoutConverter: Map[StructField, SearchField] = sparkAndNamesakeSearchFields.filterNot {
          case (k, _) => converters.exists {
            case (k2, _) => k.name.equals(k2)
          }
        }

        if (schemaFieldsWithoutConverter.nonEmpty) {
          leftMessageForSchemaFieldsWithoutConverter(schemaFieldsWithoutConverter)
        } else {
          Right(
            new SearchScan(schema, readConfig, converters)
          )
        }
      }
    }
  }

  /**
   * Create a Left that will contain a message reporting non-existing schema fields
   * @param fields non-existing schema fields
   * @param index index name
   * @return a Left instance
   */

  private def leftMessageForMissingFields(fields: Seq[String], index: String): Either[String, SearchScan] = {

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

  /**
   * Create a Left that will contain a message reporting schema fields for which a converter could not be found
   * @param fields map with keys being Spark fields and values being namesake Search fields
   * @return a Left instance
   */

  private def leftMessageForSchemaFieldsWithoutConverter(fields: Map[StructField, SearchField]): Either[String, SearchScan] = {

    val size = fields.size
    val fieldNames = fields.map {
      case (k, v) => s"${k.name} (spark type ${k.dataType.typeName}, search data type ${v.getType})"
    }.mkString("(", ", ", ")")

    Left(s"Could not find a suitable converter for $size schema fields $fieldNames")
  }
}
