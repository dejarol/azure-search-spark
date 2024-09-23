package com.github.jarol.azure.search.spark.sql.connector.core

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

/**
 * Trait to mix-in for suites that have to deal with
 *  - creating [[SearchField]](s)
 *  - creating [[StructField]](s)
 */

trait FieldFactory {

  /**
   * Create a [[StructField]] with given name and type
   * @param name name
   * @param `type` type
   * @return an instance of [[StructField]]
   */

  protected final def createStructField(name: String, `type`: DataType): StructField = StructField(name, `type`)

  /**
   * Create an array type from given inner type
   * @param `type` array inner type
   * @return an ArrayType
   */

  protected final def createArrayType(`type`: DataType): ArrayType = ArrayType(`type`)

  /**
   * Create an array field with given name and inner type
   * @param name field name
   * @param `type` array inner type
   * @return a struct field with array type
   */

  protected final def createArrayField(name: String, `type`: DataType): StructField = StructField(name, createArrayType(`type`))

  /**
   * Create a StructType from some StructFields
   * @param fields fields
   * @return a StructType
   */

  protected final def createStructType(fields: StructField*): StructType = StructType(fields)

  /**
   * Create a simple search field
   * @param name field name
   * @param `type` field type
   * @return a search field
   */

  protected final def createSearchField(name: String, `type`: SearchFieldDataType): SearchField = new SearchField(name, `type`)

  /**
   * Create a collection type using given inner type
   * @param `type` inner collection type
   * @return a search collection type
   */

  protected final def createCollectionType(`type`: SearchFieldDataType): SearchFieldDataType = SearchFieldDataType.collection(`type`)

  /**
   * Create a collection field using given name and inner type
   * @param name field name
   * @param `type` collection inner type
   * @return a search collection field
   */

  protected final def createCollectionField(name: String, `type`: SearchFieldDataType): SearchField = {

    createSearchField(
      name,
      createCollectionType(`type`)
    )
  }

  /**
   * Create a collection field with complex inner type
   * @param name name
   * @param subFields complex type subfields
   * @return a search collection fields
   */

  protected final def createComplexCollectionField(name: String, subFields: SearchField*): SearchField = {

    createCollectionField(
      name,
      SearchFieldDataType.COMPLEX
    ).setFields(subFields: _*)
  }

  /**
   * Create a complex field
   * @param name field name
   * @param fields subFields
   * @return a complex Search field
   */

  protected final def createComplexField(name: String, fields: Seq[SearchField]): SearchField = {

    createSearchField(name, SearchFieldDataType.COMPLEX)
      .setFields(
        JavaScalaConverters.seqToList(fields)
      )
  }
}
