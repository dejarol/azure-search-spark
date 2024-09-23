package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import org.apache.spark.sql.types.{DataType, StructField}

/**
 * Trait representing the mapping types from Search ecosystem to Spark ecosystem and vice versa.
 * <br>
 * There are two possible mapping types
 *  - one from Search to Spark (aka Read)
 *  - one from Spark to Search (aka Write)
 * @tparam K mapping key type
 * @tparam V mapping value type
 */

trait MappingType[K, V] {

  /**
   * Safely retrieve the converter for an atomic type.
   * <br>
   * A converter will be found if there exists either an infer schema rule or a schema conversion rule that relates to the
   * given data types
   * @param spark spark type
   * @param search search type
   * @return an optional converter for given types
   */

  protected[conversion] def converterForAtomicTypes(spark: DataType, search: SearchFieldDataType): Option[V]

  /**
   * Create a converter for data collection
   * @param internal internal converter (to use on collection inner objects)
   * @return a converter for collections
   */

  protected[conversion] def collectionConverter(sparkType: DataType, search: SearchField, internal: V): V

  /**
   * Create a converter for handling nested data objects
   * @param internal nested object mapping
   * @return a converter for handling nested data objects
   */

  protected[conversion] def complexConverter(internal: Map[K, V]): V

  /**
   * Converter for GeoPoints
   * @return converter for GeoPoints
   */

  protected[conversion] def geoPointConverter: V

  /**
   * Get the key from a Struct field
   * @param field field
   * @return a key from this field
   */

  protected[conversion] def keyOf(field: StructField): K

  /**
   * Get the name of a key
   * @param key key
   * @return key name
   */

  protected[conversion] def keyName(key: K): String
}