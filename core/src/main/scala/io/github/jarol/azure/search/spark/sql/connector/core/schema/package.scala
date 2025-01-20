package io.github.jarol.azure.search.spark.sql.connector.core

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import org.apache.spark.sql.types.DataType

import scala.language.implicitConversions

package object schema {

  /**
   * Create an instance of [[SearchFieldTypeOperations]] from a Search field type
   * @param `type` search field type
   * @return an operations instance
   */

  implicit def toSearchTypeOperations(`type`: SearchFieldDataType): SearchFieldTypeOperations = new SearchFieldTypeOperations(`type`)

  /**
   * Create an instance of [[SearchFieldOperations]] from a Search field
   * @param field search field
   * @return an operations instance
   */

  implicit def toSearchFieldOperations(field: SearchField): SearchFieldOperations = new SearchFieldOperations(field)

  /**
   * Create an instance of [[SparkTypeOperations]]
   * @param dataType input data type
   * @return an operations instance
   */

  implicit def toSparkTypeOperations(dataType: DataType): SparkTypeOperations = new SparkTypeOperations(dataType)

}

