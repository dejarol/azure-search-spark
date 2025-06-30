package io.github.dejarol.azure.search.spark.connector.core

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import org.apache.spark.sql.types.DataType

import scala.language.implicitConversions

package object schema {

  /**
   * Create an instance of [[SparkTypeAdapter]]
   * @param dataType input data type
   * @return an operations instance
   */

  implicit def toSparkTypeAdapter(dataType: DataType): SparkTypeAdapter = new SparkTypeAdapter(dataType)

  /**
   * Create an instance of [[SearchFieldOperations]] from a Search field
   * @param field search field
   * @return an operations instance
   */

  implicit def toSearchFieldOperations(field: SearchField): SearchFieldOperations = new SearchFieldOperations(field)

  /**
   * Create an instance of [[SearchFieldDataTypeAdapter]] from a Search field data type
   *
   * @param `type` search field data type
   * @return an operations instance
   */

  implicit def toSearchTypeOperations(`type`: SearchFieldDataType): SearchFieldDataTypeAdapter = new SearchFieldDataTypeAdapter(`type`)

}

