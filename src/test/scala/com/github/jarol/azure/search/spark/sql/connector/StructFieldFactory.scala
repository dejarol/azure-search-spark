package com.github.jarol.azure.search.spark.sql.connector

import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

/**
 * Trait to mix-in for suites that have to deal with [[StructField]](s)
 */

trait StructFieldFactory {

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

  protected final def createStructType(fields: StructField*): StructType = StructType(fields)
}
