package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.AtomicSchemaConversionRules
import com.github.jarol.azure.search.spark.sql.connector.schema.{SchemaUtils, toSearchTypeOperations, toSearchFieldOperations}
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

/**
 * Compatibility check for datatypes
 * @param schema schema (either inferred or used-defined)
 * @param searchFields index fields
 * @param index index name
 */

case class CompatibleTypesCheck(override protected val schema: StructType,
                                override protected val searchFields: Seq[SearchField],
                                override protected val index: String)
  extends SchemaCompatibilityCheckImpl[(SearchField, StructField)](schema, searchFields, index) {

  override protected def computeResultSet: Set[(SearchField, StructField)] = {

    // Zip schema fields with their search counterpart
    val searchFieldsAndStructFields = schema.map {
      strField => (strField, searchFields.collectFirst {
        case seaField if seaField.sameNameOf(strField) => seaField
      })
    }.collect {
      case (strField, Some(searchField)) => (searchField, strField)
    }

    // Detect those tuples where there's a datatype mismatch
    searchFieldsAndStructFields.filterNot {
      case (searchField, structField) =>
        CompatibleTypesCheck.areCompatible(searchField, structField)
    }.toSet
  }

  override protected def exceptionMessage(result: Set[(SearchField, StructField)]): String = {

    val size = result.size
    val mismatchedTypesDescription = result.map {
      case (sef, stf) => s"${sef.getName} (${sef.getType}, ${stf.dataType.typeName})"
    }.mkString("[", ",", "]")

    s"found $size fields with mismatched datatypes for index $index $mismatchedTypesDescription"
  }
}

object CompatibleTypesCheck {

  /**
   * Evaluate the data type compatibility of a SearchField and a StructField
   * @param searchField search field
   * @param structField struct field
   * @return true for compatible fields
   */

  protected[read] def areCompatible(searchField: SearchField, structField: StructField): Boolean = {

    val searchType = searchField.getType
    val sparkType = structField.dataType
    if (searchType.isAtomic) {

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
          new SearchField(null, searchType.unsafelyExtractCollectionType),
          StructField(null, elementType)
        )
        case _ => false
      }

    } else if (searchType.isComplex) {

      // Evaluate compatibility for all subfields
      sparkType match {
        case StructType(sparkSubFields) =>

          val searchSubFields: Seq[SearchField] = JavaScalaConverters.listToSeq(searchField.getFields)
          sparkSubFields.map {
            spField => (spField, searchSubFields.collectFirst {
                case seField if seField.sameNameOf(spField) => seField
              })
          }.collect {
            case (spField, Some(seField)) => (spField, seField)
          }.forall {
            case (sparkSubField, searchSubField) =>
              searchSubField.sameNameOf(sparkSubField) &&
                areCompatible(searchSubField, sparkSubField)
        }
        case _ => false
      }
    } else {
      SchemaUtils.areNaturallyCompatible(
        SchemaUtils.inferSparkTypeOf(searchField),
        structField.dataType
      )
    }
  }
}
