package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.core.schema.{SchemaUtils, toSearchFieldOperations, toSearchTypeOperations, toSparkTypeOperations}
import org.apache.spark.sql.types.{DataType, StructField}

import java.util

trait SafeMappingSupplierV2[A] {

  private type TOutput = Either[SchemaCompatibilityException, Map[FieldAdapter, A]]

  def buildMapping(
             schema: Seq[StructField],
             searchFields: Seq[SearchField],
             indexName: String
           ): TOutput = {

    if (!SchemaUtils.allSchemaFieldsExist(schema, searchFields)) {
      exceptionForMissingFields(
        SchemaUtils.getMissingSchemaFields(schema, searchFields),
        indexName
      )
    } else {
      maybeComplexObjectMapping(schema, searchFields, None)
        .left.map {
          new SchemaCompatibilityException(_)
        }
    }
  }

  private def maybeComplexObjectMapping(
                                         schema: Seq[StructField],
                                         searchFields: Seq[SearchField],
                                         prefix: Option[String]
                                       ): Either[String, Map[FieldAdapter, A]] = {

    // Match each Spark field with its namesake Search field
    val schemaAndSearchFields = SchemaUtils.matchNamesakeFields(schema, searchFields)
    val converters = schemaAndSearchFields.map {
      case (k, v) => (new FieldAdapter(k), getConverterFor(k, v, prefix))
    }.collect {
      case (k, Right(v)) => (k, v)
    }

    val incompatibleFields = schemaAndSearchFields.keySet.map(_.name)
      .diff(converters.keySet.map(_.name))

    if (incompatibleFields.nonEmpty) {
      Left("")
    } else {
      Right(converters)
    }
  }

  /**
   * Create a Left that will contain a message reporting non-existing schema fields
   *
   * @param fields non-existing schema fields
   * @param index  index name
   * @return a Left instance
   */

  private def exceptionForMissingFields(
                                         fields: Seq[String],
                                         index: String
                                       ): TOutput = {

    Left(
      SchemaCompatibilityException.forMissingFields(
        index,
        JavaScalaConverters.seqToList(fields)
      )
    )
  }

  protected def getConverterFor(
                                 schemaField: StructField,
                                 searchField: SearchField,
                                 prefix: Option[String]
                               ): Either[String, A] = {

    val (sparkType, searchFieldType) = (schemaField.dataType, searchField.getType)
    if (!searchField.sameNameOf(schemaField)) {
      Left("different name")
    } else {
      if (sparkType.isAtomic && searchFieldType.isAtomic) {
        forAtomicTypes(sparkType, searchFieldType)
      } else if (sparkType.isCollection && searchFieldType.isCollection) {
        converterForArrayType(sparkType, searchField, prefix)
      } else if (sparkType.isComplex && searchFieldType.isComplex) {
        converterForComplexType(sparkType, searchField, prefix.map(_.concat(searchField.getName)))
      } else if (sparkType.isComplex && searchFieldType.isGeoPoint) {
        converterForGeoPoint(sparkType)
      } else {
        Left("boh!")
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

  protected def forAtomicTypes(spark: DataType, search: SearchFieldDataType): Either[String, A]

  /**
   * Safely retrieve a converter for a collection type
   * <br>
   * A converter will exist only if the collection inner types are compatible
   * @param sparkType Spark array inner type
   * @param searchField Search collection inner type
   * @return a converter for collections
   */

  private def converterForArrayType(
                                     sparkType: DataType,
                                     searchField: SearchField,
                                     prefix: Option[String]
                                   ): Either[String, A] = {

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
    getConverterFor(
      StructField("array", sparkInnerType),
      searchArrayFieldMaybeWithSubFields,
      prefix.map(_.concat(searchField.getName))
    ).map(
      forCollection(sparkInnerType, searchField, _)
    )
  }

  /**
   * Create a converter for data collection
   * @param internal internal converter (to use on collection inner objects)
   * @return a converter for collections
   */

  protected def forCollection(sparkType: DataType, search: SearchField, internal: A): A

  /**
   * Safely retrieve the converter for a complex type
   * <br>
   * A converter will exist if and only if, for all Spark subfields, there exist a Search subField
   * with same name and compatible data type
   * @param sparkType spark type
   * @param searchField search field
   * @return a converter for complex fields
   */

  private def converterForComplexType(
                                       sparkType: DataType,
                                       searchField: SearchField,
                                       prefix: Option[String]
                                     ): Either[String, A] = {

    // Build a rule that wraps subField conversion rules
    val sparkSubFields = sparkType.unsafeSubFields
    val searchSubFields = JavaScalaConverters.listToSeq(searchField.getFields)
    maybeComplexObjectMapping(sparkSubFields, searchSubFields, prefix)
      .map(forComplex)
  }

  /**
   * Create a converter for handling nested data objects
   * @param internal nested object mapping
   * @return a converter for handling nested data objects
   */

  protected def forComplex(internal: Map[FieldAdapter, A]): A

  /**
   * Safely retrieve a converter for geopoints
   * <br>
   * A converter will exist if and only if given Spark types is compatible with the default geopoint schema
   * (look at [[GeoPointRule.GEO_POINT_DEFAULT_STRUCT]])
   * @param sparkType spark type
   * @return a converter for geo points
   */

  private def converterForGeoPoint(sparkType: DataType): Either[String, A] = {

    val allSubFieldsExist = sparkType.unsafeSubFields.forall {
      sf => GeoPointRule.GEO_POINT_DEFAULT_STRUCT.exists {
        geoSf => geoSf.name.equals(sf.name) && SchemaUtils.evaluateSparkTypesCompatibility(
          sf.dataType,
          geoSf.dataType
        )
      }
    }

    if (allSubFieldsExist) {
      Right(forGeoPoint)
    } else {
      Left("geoPoint!")
    }
  }

  /**
   * Converter for GeoPoints
   * @return converter for GeoPoints
   */

  protected def forGeoPoint: A
}
