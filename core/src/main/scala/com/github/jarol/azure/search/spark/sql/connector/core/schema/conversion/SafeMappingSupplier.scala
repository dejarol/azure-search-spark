package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.core.schema.{SchemaCompatibilityException, SchemaUtils}
import org.apache.spark.sql.types.StructField

/**
 * Supplier of mappings from Spark internal rows to Search documents and vice versa.
 * <br>
 * The output mapping will be a map with
 *  - keys being Spark column identifiers
 *  - values being converters for extracting data from either SearchDocuments or Spark internal rows
 * @tparam K mapping key type
 * @tparam V mapping value type
 */

case class SafeMappingSupplier[K, V](private val delegate: MappingType[K, V]) {

  private lazy val converterSupplier = SafeConverterSupplier(delegate)

  private type MaybeMapping = Either[SchemaCompatibilityException, Map[K, V]]

  /**
   * Safely create a mapping that will represent how to convert a Search document to a Spark internal row or vice versa.
   * <br>
   * The mapping will be build only if Spark schema and Search schema are compatible
   * @param schema schema (either inferred or used-defined)
   * @param searchFields Search index fields
   * @param indexName index name
   * @return either a string reporting the incompatibility among the schemas or the conversion mapping
   */

  final def get(
                 schema: Seq[StructField],
                 searchFields: Seq[SearchField],
                 indexName: String
               ): MaybeMapping = {

    // Retrieve the set of schema fields that do not exist on Search index
    if (!SchemaUtils.allSchemaFieldsExist(schema, searchFields)) {
      exceptionForMissingFields(
        SchemaUtils.getMissingSchemaFields(schema, searchFields),
        indexName
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
        exceptionForNonCompatibleFields(nonCompatibleFields)
      } else {

        // Retrieve a converter for each schema field
        val converters: Map[K, V] = sparkAndNamesakeSearchFields.map {
          case (k, v) => (delegate.keyOf(k), converterSupplier.get(k, v))
        }.collect {
          case (k, Some(v)) => (k, v)
        }

        // Detect those schema fields for which a converter could not be found
        val schemaFieldsWithoutConverter: Map[StructField, SearchField] = sparkAndNamesakeSearchFields.filterNot {
          case (structField, _) => converters.exists {
            case (key, _) => delegate.keyName(key).equalsIgnoreCase(structField.name)
          }
        }

        if (schemaFieldsWithoutConverter.nonEmpty) {
          exceptionForSchemaFieldsWithoutConverter(schemaFieldsWithoutConverter)
        } else {
          Right(converters)
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

  private def exceptionForMissingFields(fields: Seq[String], index: String): MaybeMapping = {

    Left(
      SchemaCompatibilityException.forMissingFields(
        index,
        JavaScalaConverters.seqToList(fields)
      )
    )
  }

  /**
   * Create a Left that will contain a message reporting schema fields with incompatible datatypes
   * @param fields map with keys being Spark fields and values being namesake Search fields
   * @return a Left instance
   */

  private def exceptionForNonCompatibleFields(fields: Map[StructField, SearchField]): MaybeMapping = {

    Left(
      SchemaCompatibilityException.forNonCompatibleFields(
        JavaScalaConverters.scalaMapToJava(
          fields
        )
      )
    )
  }

  /**
   * Create a Left that will contain a message reporting schema fields for which a converter could not be found
   * @param fields map with keys being Spark fields and values being namesake Search fields
   * @return a Left instance
   */

  private def exceptionForSchemaFieldsWithoutConverter(fields: Map[StructField, SearchField]):MaybeMapping = {

    Left(
      SchemaCompatibilityException.forSchemaFieldsWithoutConverter(
        JavaScalaConverters.scalaMapToJava(
          fields
        )
      )
    )
  }
}
