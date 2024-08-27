package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.schema.{SchemaUtils, toSearchTypeOperations}
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.AtomicSchemaConversionRules
import org.apache.spark.sql.types.{StructField, StructType}

case class AllDatatypesCompatibleCheck(override protected val schema: StructType,
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
      case (searchField, structField) => AllDatatypesCompatibleCheck
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

object AllDatatypesCompatibleCheck {

  def areCompatible(searchField: SearchField, structField: StructField): Boolean = {

    val searchType = searchField.getType
    SchemaUtils.areCompatible(
      SchemaUtils.inferSparkTypeOf(searchField),
      structField.dataType
    ) || (
      searchType.isAtomic &&
        AtomicSchemaConversionRules.existsRuleFor(
          structField.dataType,
          searchType
        )
      )
  }
}
