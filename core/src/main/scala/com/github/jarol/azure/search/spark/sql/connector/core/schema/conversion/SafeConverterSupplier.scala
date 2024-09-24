package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.core.schema.{SchemaUtils, toSearchFieldOperations, toSearchTypeOperations, toSparkTypeOperations}
import org.apache.spark.sql.types.{DataType, StructField}

import java.util

/**
 * Trait for supplying converters from Search ecosystem to Spark ecosystem and vice versa.
 * <br>
 * There are two possible converter types
 *  - one for reading data from Search to Spark
 *  - one for writing data from Spark to Search
 * @tparam C converter type
 */

trait SafeConverterSupplier[C] {

  /**
   * Get a converter for transforming data from a Search-defined index field to a Spark column, or vice versa
   * @param structField Spark column definition
   * @param searchField Search field definition
   * @return a non-empty converter in case of compatible column-field definitions
   */

  final def get(structField: StructField, searchField: SearchField): Option[C] = {

    val (sparkType, searchFieldType) = (structField.dataType, searchField.getType)
    if (!searchField.sameNameOf(structField)) {
      None
    } else {
      if (sparkType.isAtomic && searchFieldType.isAtomic) {
        forAtomicTypes(sparkType, searchFieldType)
      } else if (sparkType.isCollection && searchFieldType.isCollection) {
        converterForArrayType(sparkType, searchField)
      } else if (sparkType.isComplex && searchFieldType.isComplex) {
        converterForComplexType(sparkType, searchField)
      } else if (sparkType.isComplex && searchFieldType.isGeoPoint) {
        converterForGeoPoint(sparkType)
      } else {
        None
      }
    }
  }

  /**
   * Safely retrieve the converter between two atomic types.
   * <br>
   * A converter will be found if there exists either an infer schema rule or a schema conversion rule that relates to the
   * given data types
   * @param spark Spark type
   * @param search Search type
   * @return an optional converter for given types
   */

  protected def forAtomicTypes(spark: DataType, search: SearchFieldDataType): Option[C]

  /**
   * Safely retrieve a converter for a collection type
   * <br>
   * A converter will exist only if the collection inner types are compatible
   * @param sparkType Spark array inner type
   * @param searchField Search collection inner type
   * @return a converter for collections
   */

  private def converterForArrayType(sparkType: DataType, searchField: SearchField): Option[C] = {

    // In inner type is complex, we have to bring in subFields definition from the wrapping Search field
    val (sparkInnerType, searchInnerType) = (sparkType.unsafeCollectionInnerType, searchField.getType.unsafeCollectionInnerType)
    val maybeSubFields: Option[util.List[SearchField]] = if (searchInnerType.isComplex) {
      Some(searchField.getFields)
    } else None

    val searchArrayField = new SearchField("array", searchInnerType)
    val searchArrayFieldMaybeWithSubFields = maybeSubFields match {
      case Some(value) => searchArrayField.setFields(value)
      case None => searchArrayField
    }
    // Get the converter recursively
    get(
      StructField("array", sparkInnerType),
      searchArrayFieldMaybeWithSubFields
    ).map(
      forCollection(sparkInnerType, searchField, _)
    )
  }

  /**
   * Create a converter for data collection
   * @param internal internal converter (to use on collection inner objects)
   * @return a converter for collections
   */

  protected def forCollection(sparkType: DataType, search: SearchField, internal: C): C

  /**
   * Safely retrieve the converter for a complex type
   * <br>
   * A converter will exist if and only if, for all Spark subfields, there exist a Search subField
   * with same name and compatible data type
   * @param sparkType spark type
   * @param searchField search field
   * @return a converter for complex fields
   */

  private def converterForComplexType(sparkType: DataType, searchField: SearchField): Option[C] = {

    // Build a rule that wraps subField conversion rules
    val sparkSubFields = sparkType.unsafeSubFields
    val searchSubFields = JavaScalaConverters.listToSeq(searchField.getFields)

    // Compute the converter for to each subfield
    val convertersForSubFields: Map[StructField, C] = SchemaUtils
      .matchNamesakeFields(sparkSubFields, searchSubFields)
      .map {
        case (k, v) => (k, get(k, v))
      }.collect {
        case (k, Some(v)) => (k, v)
      }

    // For each subfield, there should exist a search subfields with same name and a converter should exist
    val allSubFieldsExist = SchemaUtils.allSchemaFieldsExist(sparkSubFields, searchSubFields)
    val allSubFieldsHaveAConverter = sparkSubFields.forall {
      subField =>
        convertersForSubFields.exists {
          case (key, _) => key.name.equalsIgnoreCase(subField.name)
        }
    }

    // If so, create the Complex converter
    if (allSubFieldsExist && allSubFieldsHaveAConverter) {
      Some(forComplex(convertersForSubFields))
    } else {
      None
    }
  }

  /**
   * Create a converter for handling nested data objects
   * @param internal nested object mapping
   * @return a converter for handling nested data objects
   */

  protected def forComplex(internal: Map[StructField, C]): C

  /**
   * Safely retrieve a converter for geopoints
   * <br>
   * A converter will exist if and only if given Spark types is compatible with the default geopoint schema
   * (look at [[GeoPointRule.GEO_POINT_DEFAULT_STRUCT]])
   * @param sparkType spark type
   * @return a converter for geo points
   */

  private def converterForGeoPoint(sparkType: DataType): Option[C] = {

    val allSubFieldsExist = sparkType.unsafeSubFields.forall {
      sf => GeoPointRule.GEO_POINT_DEFAULT_STRUCT.exists {
        geoSf => geoSf.name.equals(sf.name) && SchemaUtils.evaluateSparkTypesCompatibility(
          sf.dataType,
          geoSf.dataType
        )
      }
    }

    if (allSubFieldsExist) {
      Some(forGeoPoint)
    } else {
      None
    }
  }

  /**
   * Converter for GeoPoints
   * @return converter for GeoPoints
   */

  protected def forGeoPoint: C
}