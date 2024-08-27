package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchField
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Schema compatibility check that will ensure that each Spark field name exists on a Search index
 * @param schema Spark schema (either inferred or user-specified)
 * @param searchFields Search index fields
 * @param index index name
 */

case class FieldExistsCheck(override protected val schema: StructType,
                            override protected val searchFields: Seq[SearchField],
                            override protected val index: String)
  extends SchemaCompatibilityCheckImpl[StructField](schema, searchFields, index) {

  override protected def computeResult: Set[StructField] = {

    schema.filterNot {
      structField => searchFields.exists {
        searchField => structField.name.equalsIgnoreCase(searchField.getName)
      }
    }.toSet
  }

  override protected def exceptionMessage(result: Set[StructField]): String = {

    val size = result.size
    val missingFields = result.map(_.name).mkString("(", ", ", ")")
    s"found $size schema field(s) that do not exist on search index $index $missingFields"
  }
}