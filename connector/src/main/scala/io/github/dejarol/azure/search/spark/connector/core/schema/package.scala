package io.github.dejarol.azure.search.spark.connector.core

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import org.apache.spark.sql.types.{DataType, StructField}

import scala.language.implicitConversions

package object schema {

  /**
   * Converts a Spark field into its 'operation' counterpart, in order to use a unified API
   * @param field a Spark field
   * @return an instance of [[StructFieldOperations]]
   */

  implicit def toSparkFieldOperations(field: StructField): StructFieldOperations = StructFieldOperations(field)

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

