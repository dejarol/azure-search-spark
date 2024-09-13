package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.{SearchField, SearchIndex}
import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters
import org.apache.spark.sql.types.StructField

case class CreateIndexOptions(name: String,
                              keyField: String,
                              filterableFields: Option[Seq[String]],
                              sortableFields: Option[Seq[String]]) {

  def getSearchIndex(schema: Seq[StructField]): SearchIndex = {

    val indexFields: Seq[SearchField] = Seq.empty
    new SearchIndex(name)
      .setFields(
        JavaScalaConverters.seqToList(
          indexFields
        )
      )
  }
}
