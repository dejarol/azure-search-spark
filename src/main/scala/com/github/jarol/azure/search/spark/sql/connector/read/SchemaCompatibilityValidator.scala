package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.schema.{SchemaUtils, toSearchFieldOperations, toSearchTypeOperations}
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.AtomicSchemaConversionRules
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

object SchemaCompatibilityValidator {

  def computeMismatches(schema: Seq[StructField], searchFields: Seq[SearchField]): Option[Seq[String]] = {

    val existsASearchFieldWithSameName: StructField => Boolean = strField => searchFields.exists {
      seField =>
        seField.sameNameOf(strField)
    }

    val missingSchemaFields: Seq[StructField] = schema.filterNot(existsASearchFieldWithSameName)
    val schemaFieldsWithNonCompatibleTypes: Seq[(StructField, SearchFieldDataType)] = schema
      .filter(existsASearchFieldWithSameName)
      .map {
        strField => (
          strField,
          searchFields.collectFirst {
            case seField if seField.sameNameOf(strField) => seField
        })
    }.collect {
        case (strField, Some(seField)) if !areCompatible(seField, strField) =>
          (strField, seField.getType)
      }

    None
  }

  /**
   * Evaluate the data type compatibility of a SearchField and a StructField
   * @param searchField search field
   * @param structField struct field
   * @return true for compatible fields
   */

  def areCompatible(searchField: SearchField, structField: StructField): Boolean = {

    val (searchType, sparkType) = (searchField.getType, structField.dataType)
    val sameName: Boolean = searchField.sameNameOf(structField)
    val compatibleDataType: Boolean = if (searchType.isAtomic) {

      // They should be either naturally compatible or a suitable conversion rule should exist
      SchemaUtils.areNaturallyCompatible(
        SchemaUtils.inferSparkTypeOf(searchField),
        sparkType
      ) || AtomicSchemaConversionRules.existsRuleFor(
        sparkType,
        searchType
      )
    } else if (searchType.isCollection) {

      // Evaluate compatibility on the inner type
      sparkType match {
        case ArrayType(elementType, _) => areCompatible(
          new SearchField("array", searchType.unsafelyExtractCollectionType),
          StructField("array", elementType)
        )
        case _ => false
      }

    } else if (searchType.isComplex) {

      // Evaluate compatibility for all subfields
      sparkType match {
        case StructType(sparkSubFields) =>

          val searchSubFields: Seq[SearchField] = JavaScalaConverters.listToSeq(searchField.getFields)

          // For each Spark subfield
          // [1] a Search subfield with same name should exist
          // [2] the Spark type and the Search type should be compatible
          val forAllSparkSubFieldsExistsASearchSubFieldWithSameName: Boolean = sparkSubFields.forall {
            spsField =>
              searchSubFields.exists {
                sesField => sesField.sameNameOf(spsField)
            }
          }

          val allSubfieldsAreCompatible: Boolean = sparkSubFields.map {
            spField =>
              // Collect first matching (i.e. with same name) field
              (spField, searchSubFields.collectFirst {
                case seField if seField.sameNameOf(spField) => seField
              })
          }.collect {
            // Collect cases where a matching field exists
            case (spField, Some(seField)) => (spField, seField)
          }.forall {
            case (sparkSubField, searchSubField) =>
              areCompatible(searchSubField, sparkSubField)
          }

          forAllSparkSubFieldsExistsASearchSubFieldWithSameName && allSubfieldsAreCompatible
        case _ => false
      }
    } else {
      SchemaUtils.areNaturallyCompatible(
        SchemaUtils.inferSparkTypeOf(searchField),
        structField.dataType
      )
    }

    sameName && compatibleDataType
  }
}

