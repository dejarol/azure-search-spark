package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.AtomicSchemaConversionRules
import com.github.jarol.azure.search.spark.sql.connector.schema.{SchemaUtils, toSearchFieldOperations, toSearchTypeOperations}
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

object SchemaValidator {

  def validate(schema: Seq[StructField], searchFields: Seq[SearchField]): Option[SchemaCompatibilityException] = {

    val existsASearchFieldWithSameName: StructField => Boolean = strField => searchFields.exists {
      seField =>
        seField.sameNameOf(strField)
    }

    // Schema fields that cannot be mapped to Search fields
    val missingSchemaFields: Seq[String] = schema.collect {
      case field if !existsASearchFieldWithSameName(field) => field.name
    }

    // Schema fields that can be mapped to a Search field but with non-compatible datatype
    val schemaFieldsWithNonCompatibleTypes: Map[StructField, SearchField] = schema
      .collect {
        case strField if existsASearchFieldWithSameName(strField) => (
          strField, searchFields.collectFirst {
            case seField if seField.sameNameOf(strField) => seField
          }
        )
      }.collect {
        case (strField, Some(seField)) if !areCompatible(seField, strField) =>
          (strField, seField)
      }.toMap

    if (missingSchemaFields.isEmpty && schemaFieldsWithNonCompatibleTypes.isEmpty) {
      None
    } else {
      Some(
        new SchemaCompatibilityException(
          missingSchemaFields,
          schemaFieldsWithNonCompatibleTypes
        )
      )
    }
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

