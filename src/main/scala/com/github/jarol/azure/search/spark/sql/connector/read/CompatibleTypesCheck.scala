package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.AtomicSchemaConversionRules
import com.github.jarol.azure.search.spark.sql.connector.schema.{SchemaUtils, toSearchTypeOperations}
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

case class CompatibleTypesCheck(override protected val schema: StructType,
                                override protected val searchFields: Seq[SearchField],
                                override protected val index: String)
  extends SchemaCompatibilityCheckImpl[(SearchField, StructField)](schema, searchFields, index) {

  override protected def computeResult: Set[(SearchField, StructField)] = {

    // Zip search fields with their equivalent struct fields
    val searchFieldsAndStructFields = searchFields.filter {
      searchField => schema.exists {
        structField => searchField.getName.equalsIgnoreCase(structField.name)
      }
    }.sortBy(_.getName).zip(schema.sortBy(_.name))

    // Detect those tuples where there's a datatype mismatch
    searchFieldsAndStructFields.filterNot {
      case (searchField, structField) => CompatibleTypesCheck
        .areCompatible(searchField, structField)
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

  def areCompatible(searchField: SearchField, structField: StructField): Boolean = {

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
        case StructType(fields) => JavaScalaConverters
          .listToSeq(searchField.getFields)
          .sortBy(_.getName).zip(fields.sortBy(_.name)).forall {
            case (searchSubField, sparkSubField) => areCompatible(searchSubField, sparkSubField)
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
