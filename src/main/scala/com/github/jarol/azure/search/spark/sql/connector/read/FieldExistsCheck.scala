package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.schema._
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Schema compatibility check that will ensure that for each schema fields exists
 * a Search field with same name
 * @param schema Spark schema (either inferred or user-specified)
 * @param searchFields Search index fields
 * @param index index name
 */

case class FieldExistsCheck(override protected val schema: StructType,
                            override protected val searchFields: Seq[SearchField],
                            override protected val index: String)
  extends SchemaCompatibilityCheckImpl[StructField](schema, searchFields, index) {

  override protected def computeResultSet: Set[StructField] = {

    schema.filterNot {
      structField => searchFields.exists {
        searchField => searchField.sameNameOf(structField)
      }
    }.toSet
  }

  override protected def exceptionMessage(result: Set[StructField]): String = {

    val size = result.size
    val missingFields = result.map(_.name).mkString("(", ", ", ")")
    s"found $size schema field(s) that do not exist on search index $index $missingFields"
  }
}