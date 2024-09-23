package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.core.schema.{SchemaUtils, toSearchFieldOperations, toSearchTypeOperations, toSparkTypeOperations}
import org.apache.spark.sql.types.{DataType, StructField}

/**
 * Safe supplier of data converter from Spark ecosystem to Search ecosystem and vice versa
 * @tparam K key type for internal mapping required by a complex converter
 * @tparam V converter type
 */

case class SafeConverterSupplier[K, V](private val delegate: MappingType[K, V]) {

  /**
   * Get a converter for transforming data from a Search-defined index field to a Spark column, or vice versa
   * @param structField Spark column definition
   * @param searchField Search field definition
   * @return a non-empty converter in case of compatible column-field definitions
   */

  final def get(structField: StructField, searchField: SearchField): Option[V] = {

    val (sparkType, searchFieldType) = (structField.dataType, searchField.getType)
    if (!searchField.sameNameOf(structField)) {
      None
    } else {
      if (sparkType.isAtomic && searchFieldType.isAtomic) {
        delegate.converterForAtomicTypes(sparkType, searchFieldType)
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
   * Safely retrieve a converter for a collection type
   * <br>
   * A converter will exist only if the collection inner types are compatible
   * @param sparkType Spark array inner type
   * @param searchField Search collection inner type
   * @return a converter for collections
   */

  private def converterForArrayType(sparkType: DataType, searchField: SearchField): Option[V] = {

    // In inner type is complex, we have to bring in subFields definition from the wrapping Search field
    val (sparkInnerType, searchInnerType) = (sparkType.unsafeCollectionInnerType, searchField.getType.unsafeCollectionInnerType)
    val searchInnerField: SearchField = if (searchInnerType.isComplex) {
      new SearchField("array", SearchFieldDataType.COMPLEX)
        .setFields(searchField.getFields)
    } else {
      new SearchField("array", searchInnerType)
    }

    // Get the converter recursively
    get(
      StructField("array", sparkInnerType),
      searchInnerField
    ).map(
      delegate.collectionConverter(sparkInnerType, searchField, _)
    )
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
    val sparkSubFields = sparkType.unsafeSubFields
    val searchSubFields = JavaScalaConverters.listToSeq(searchField.getFields)

    // Compute the converter for to each subfield
    val convertersForSubFields: Map[K, V] = SchemaUtils
      .matchNamesakeFields(sparkSubFields, searchSubFields)
      .map {
        case (k, v) => (delegate.keyOf(k), get(k, v))
      }.collect {
        case (k, Some(v)) => (k, v)
      }

    // For each subfield, there should exist a search subfields with same name and a converter should exist
    val allSubFieldsExist = SchemaUtils.allSchemaFieldsExist(sparkSubFields, searchSubFields)
    val allSubFieldsHaveAConverter = sparkSubFields.forall {
      subField =>
        convertersForSubFields.exists {
          case (key, _) => delegate.keyName(key).equalsIgnoreCase(subField.name)
        }
    }

    // If so, create the Complex converter
    if (allSubFieldsExist && allSubFieldsHaveAConverter) {
      Some(delegate.complexConverter(convertersForSubFields))
    } else {
      None
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

    val allSubFieldsExist = sparkType.unsafeSubFields.forall {
      sf => GeoPointRule.GEO_POINT_DEFAULT_STRUCT.exists {
        geoSf => geoSf.name.equals(sf.name) && SchemaUtils.evaluateSparkTypesCompatibility(
          sf.dataType,
          geoSf.dataType
        )
      }
    }

    if (allSubFieldsExist) {
      Some(delegate.geoPointConverter)
    } else {
      None
    }
  }
}
