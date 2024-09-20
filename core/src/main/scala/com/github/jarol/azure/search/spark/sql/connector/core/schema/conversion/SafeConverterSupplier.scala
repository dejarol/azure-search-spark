package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.core.schema.{SchemaUtils, toSearchFieldOperations, toSearchTypeOperations}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

/**
 * Safe supplier of data converter from Spark ecosystem to Search ecosystem and vice versa
 * @tparam K key type for internal mapping required by a complex converter
 * @tparam V converter type
 */

trait SafeConverterSupplier[K, V] {

  /**
   * Create a converter for data collection
   * @param internal internal converter (to use on collection inner objects)
   * @return a converter for collections
   */

  protected def collectionConverter(internal: V): V

  /**
   * Create a converter for handling nested data objects
   * @param internal nested object mapping
   * @return a converter for handling nested data objects
   */

  protected def complexConverter(internal: Map[K, V]): V

  /**
   * Converter for GeoPoints
   * @return converter for GeoPoints
   */

  protected def geoPointConverter: V

  /**
   * Get the key from a Struct field
   * @param field field
   * @return a key from this field
   */

  protected[conversion] def keyFrom(field: StructField): K

  /**
   * Get the name of a key
   * @param key key
   * @return key name
   */

  protected[conversion] def nameFrom(key: K): String

  /**
   * Safely retrieve the converter given the Spark and Search definition
   * @param structField Spark field
   * @param searchField Search field
   * @return a non-empty converter if the fields have same and have compatible data types
   */

  final def getConverter(structField: StructField, searchField: SearchField): Option[V] = {

    val (sparkType, searchFieldType) = (structField.dataType, searchField.getType)
    if (!searchField.sameNameOf(structField)) {
      None
    } else {
      if (searchFieldType.isAtomic) {
        converterForAtomicTypes(sparkType, searchFieldType)
      } else if (searchFieldType.isCollection) {
        converterForArrayType(sparkType, searchFieldType)
      } else if (searchFieldType.isComplex) {
        converterForComplexType(sparkType, searchField)
      } else if (searchFieldType.isGeoPoint) {
        converterForGeoPoint(sparkType)
      } else {
        None
      }
    }
  }

  /**
   * Safely retrieve the converter for an atomic type.
   * <br>
   * A converter will be found if there exists either an infer schema rule or a schema conversion rule that relates to the
   * given data types
   * @param spark spark type
   * @param search search type
   * @return an optional converter for given types
   */

  protected def converterForAtomicTypes(spark: DataType, search: SearchFieldDataType): Option[V]

  /**
   * Safely retrieve a converter for a collection type
   * <br>
   * A converter will exist only if the collection inner types are compatible
   * @param sparkType Spark array inner type
   * @param searchType Search collection inner type
   * @return a converter for collections
   */

  private def converterForArrayType(sparkType: DataType, searchType: SearchFieldDataType): Option[V] = {

    // Evaluate rule for the inner type
    val searchInnerType = searchType.unsafeCollectionInnerType
    sparkType match {
      case ArrayType(sparkInternalType, _) => getConverter(
        StructField("array", sparkInternalType),
        new SearchField("array", searchInnerType)
      ).map(collectionConverter)
      case _ => None
    }
  }

  /**
   * Safely retrieve the converter for a complex type
   * <br>
   * A converter will exist if and only if, for all Spark subfields, there exist a Search subField
   * with same name and compatible data type
   * @param sparkType spark type
   * @param searchField search field
   * @return a converter for complex fields
   */

  private def converterForComplexType(sparkType: DataType, searchField: SearchField): Option[V] = {

    // Build a rule that wraps subField conversion rules
    sparkType match {
      case StructType(sparkSubFields) =>

        // Compute the converter for to each subfield
        val searchSubFields = JavaScalaConverters.listToSeq(searchField.getFields)
        val convertersMap: Map[K, V] = SchemaUtils
          .matchNamesakeFields(sparkSubFields, searchSubFields)
          .map {
            case (k, v) => (keyFrom(k), getConverter(k, v))
          }.collect {
            case (k, Some(v)) => (k, v)
          }

        // For each subfield, there should exist a search subfields with same name and a converter should exist
        val allSubFieldsExist = SchemaUtils.allSchemaFieldsExist(sparkSubFields, searchSubFields)
        val allSubFieldsHaveAConverter = sparkSubFields.forall {
          subField =>
            convertersMap.exists {
              case (key, _) => nameFrom(key).equalsIgnoreCase(subField.name)
            }
        }

        // If so, create the Complex converter
        if (allSubFieldsExist && allSubFieldsHaveAConverter) {
          Some(complexConverter(convertersMap))
        } else {
          None
        }
      case _ => None
    }
  }

  /**
   * Safely retrieve a converter for geopoints
   * <br>
   * A converter will exist if and only if given Spark types is compatible with the default geopoint schema
   * (look at [[GeoPointRule.GEO_POINT_DEFAULT_STRUCT]])
   * @param sparkType spark type
   * @return a converter for geo points
   */

  private def converterForGeoPoint(sparkType: DataType): Option[V] = {

    sparkType match {
      case StructType(subFields) =>

        val allSubFieldsExist = subFields.forall {
          sf => GeoPointRule.GEO_POINT_DEFAULT_STRUCT.exists {
            geoSf => geoSf.name.equals(sf.name) && SchemaUtils.evaluateSparkTypesCompatibility(
              sf.dataType,
              geoSf.dataType
            )
          }
        }

        if (allSubFieldsExist) {
          Some(geoPointConverter)
        } else {
          None
        }

      case _=> None
    }
  }
}
