package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType, SearchIndex}
import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters
import org.apache.spark.sql.types.StructField

case class CreateIndexOptions(name: String,
                              keyField: String,
                              filterableFields: Option[Seq[String]],
                              sortableFields: Option[Seq[String]],
                              indexActionColumn: Option[String]) {

  def getSearchIndex(schema: Seq[StructField]): SearchIndex = {

    val indexFields: Seq[SearchField] = indexActionColumn.map {
      name => schema.filterNot {
        sf => sf.name.equalsIgnoreCase(name)
      }
    }.getOrElse(schema).map {
      sf => new SearchField(
        sf.name,
        SearchFieldDataType.INT32
      )
    }
    new SearchIndex(name)
      .setFields(
        JavaScalaConverters.seqToList(
          indexFields
        )
      )
  }
}
