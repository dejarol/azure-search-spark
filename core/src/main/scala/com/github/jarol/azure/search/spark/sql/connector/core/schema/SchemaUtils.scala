package com.github.jarol.azure.search.spark.sql.connector.core.schema

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.GeoPointType
import com.github.jarol.azure.search.spark.sql.connector.core.{DataTypeException, JavaScalaConverters}
import org.apache.spark.sql.types._

/**
 * Utilities for dealing with both Spark and Search types
 */

object SchemaUtils {

  /**
   * Return the Spark equivalent [[DataType]] for a search field
   * @param searchField a search field
   * @throws DataTypeException if given search type is not supported
   * @return the equivalent Spark data type for given search field
   */

  @throws[DataTypeException]
  final def inferSparkTypeOf(searchField: SearchField): DataType = {

    val searchType = searchField.getType
    if (searchType.isAtomic) {
      inferSparkAtomicType(searchType)
    } else if (searchType.isCollection) {

      // Extract collection inner type
      ArrayType(
        inferSparkTypeOf(
          new SearchField("array", searchType.unsafeCollectionInnerType)
        ),
        containsNull = true
      )
    } else if (searchType.isComplex) {

      // Extract subfields,
      // convert them into StructFields
      // and create a StructType
      StructType(
        JavaScalaConverters.listToSeq(searchField.getFields)
          .map { searchField =>
            StructField(
              searchField.getName,
              inferSparkTypeOf(searchField)
            )
          }
      )
    } else if (searchType.isGeoPoint) {
      GeoPointType.SCHEMA
    } else {
      throw DataTypeException.forUnsupportedSearchType(searchType)
    }
  }

  /**
   * Infer the Spark type of an atomic Search type
   * @param searchType search type
   * @throws DataTypeException for unsupported Search types
   * @return the inferred Spark data type
   */

  @throws[DataTypeException]
  private def inferSparkAtomicType(searchType: SearchFieldDataType): DataType = {

    if (searchType.isString) {
      DataTypes.StringType
    } else if (searchType.isNumeric) {
      searchType match {
        case SearchFieldDataType.INT32 => DataTypes.IntegerType
        case SearchFieldDataType.INT64 => DataTypes.LongType
        case SearchFieldDataType.DOUBLE => DataTypes.DoubleType
        case SearchFieldDataType.SINGLE => DataTypes.FloatType
        case _ => throw DataTypeException.forUnsupportedSearchType(searchType)
      }
    } else if (searchType.isBoolean) {
      DataTypes.BooleanType
    } else if (searchType.isDateTime) {
      DataTypes.TimestampType
    } else {
      throw DataTypeException.forUnsupportedSearchType(searchType)
    }
  }

  /**
   * Convert a search field to a [[StructField]]
   * @param searchField search field
   * @return the equivalent [[StructField]] of this search field
   */

  protected[schema] def toStructField(searchField: SearchField): StructField = {

    StructField(
      searchField.getName,
      inferSparkTypeOf(searchField)
    )
  }

  /**
   * Convert a SearchIndex schema to a [[StructType]]
   * @param fields search index fields
   * @return the schema of the search index
   */

  final def toStructType(fields: Seq[SearchField]): StructType = {

    StructType(
      fields.map(toStructField)
    )
  }

  /**
   * Evaluate natural data type compatibility according to the following behavior
   *  - for atomic types, the natural equality test is employed
   *  - for array types, the inner types should be compatible
   *  - for struct types, all subfields should be compatible (same size and same type for all subfields)
   * @param first first type
   * @param second second type
   * @return true if the two types are compatible
   */

  final def evaluateSparkTypesCompatibility(first: DataType, second: DataType): Boolean = {

    (first, second) match {
      case (f: ArrayType, s: ArrayType) => evaluateSparkTypesCompatibility(f.elementType, s.elementType)
      case (f: StructType, s: StructType) => f.forall {
        ff => s.exists {
          ss => ss.name.equals(ff.name) &&
            evaluateSparkTypesCompatibility(ff.dataType, ss.dataType)
        }
      }
      case _ => first.equals(second)
    }
  }

  /**
   * Evaluate whether all schema fields exist in a Search schema
   * <br>
   * For a schema field to exist, a namesake Search field should exist
   * @param schema schema fields
   * @param searchFields search fields
   * @return true if for all schema fields a namesake Search field exist
   */

  final def allSchemaFieldsExist(schema: Seq[StructField], searchFields: Seq[SearchField]): Boolean = {

    schema.forall {
      spField => searchFields.exists {
        seField => seField.sameNameOf(spField)
      }
    }
  }

  /**
   * Retrieve the name of schema fields that do not have a namesake Search fields
   * @param schema schema fields
   * @param searchFields search fields
   * @return missing schema fields
   */

  final def getMissingSchemaFields(schema: Seq[StructField], searchFields: Seq[SearchField]): Seq[String] = {

    schema.collect {
      case spField if !searchFields.exists {
        seField => seField.sameNameOf(spField)
      } => spField.name
    }
  }

  /**
   * Zip each schema field with the namesake Search field (if it exists).
   * The output will contain only schema fields that have a Search counterpart
   * @param schema schema
   * @param searchFields Search fields
   * @return a map with keys being schema fields and values being the namesake Search fields
   */

  final def matchNamesakeFields(schema: Seq[StructField], searchFields: Seq[SearchField]): Map[StructField, SearchField] = {

    schema.map {
      spField => (
        spField,
        searchFields.collectFirst {
          case seField if seField.sameNameOf(spField) => seField
        }
      )
    }.collect {
      case (k, Some(v)) => (k, v)
    }.toMap
  }

  /**
   * Infer the [[SearchFieldDataType]] for a Spark type
   * @param dataType Spark type
   * @throws DataTypeException for unsupported data types
   * @return the inferred Search type
   */

  @throws[DataTypeException]
  final def inferSearchTypeFor(dataType: DataType): SearchFieldDataType = {

    if (dataType.isAtomic) {
      inferSearchAtomicType(dataType)
    } else if (dataType.isCollection) {
      SearchFieldDataType.collection(
        inferSearchTypeFor(dataType.unsafeCollectionInnerType)
      )
    } else if (dataType.isComplex) {

      // If compatible with GeoPoint, use Geography point Search data type
      val compatibleWithGeoPoint = evaluateSparkTypesCompatibility(dataType, GeoPointType.SCHEMA)
      if (compatibleWithGeoPoint) {
        SearchFieldDataType.GEOGRAPHY_POINT
      } else SearchFieldDataType.COMPLEX
    } else {
      throw new DataTypeException(s"Unsupported Spark type ($dataType)")
    }
  }

  /**
   * Infer the Search type for an atomic Spark type
   * @param dataType Spark type
   * @throws DataTypeException for unsupported data types
   * @return the inferred Search type
   */

  @throws[DataTypeException]
  private def inferSearchAtomicType(dataType: DataType): SearchFieldDataType = {

    if (dataType.isString) {
      SearchFieldDataType.STRING
    } else if (dataType.isNumeric) {
      dataType match {
        case DataTypes.IntegerType => SearchFieldDataType.INT32
        case DataTypes.LongType => SearchFieldDataType.INT64
        case DataTypes.DoubleType => SearchFieldDataType.DOUBLE
        case DataTypes.FloatType => SearchFieldDataType.SINGLE
        case _ => throw DataTypeException.forUnsupportedSparkType(dataType)
      }
    } else if (dataType.isBoolean) {
      SearchFieldDataType.BOOLEAN
    } else if (dataType.isDateTime) {
      SearchFieldDataType.DATE_TIME_OFFSET
    } else {
      throw DataTypeException.forUnsupportedSparkType(dataType)
    }
  }

  /**
   * Convert a Spark field into a Search field
   * @param structField Spark fields
   * @throws DataTypeException for Spark fields with unsupported types
   * @return the equivalent Search field
   */

  @throws[DataTypeException]
  final def toSearchField(structField: StructField): SearchField = {

    val (name, dType) = (structField.name, structField.dataType)
    if (dType.isAtomic) {
      new SearchField(name, inferSearchTypeFor(dType))
    } else if (dType.isCollection) {

      // If the inner type is complex, we should add subFields to newly created field
      val searchField = new SearchField(
        name,
        SearchFieldDataType.collection(
          inferSearchTypeFor(dType.unsafeCollectionInnerType)
        )
      )

      // Optional subfields
      val maybeSearchSubFields: Option[Seq[SearchField]] = dType
        .unsafeCollectionInnerType
        .safeSubFields.map {
          v => v.map(toSearchField)
      }

      // Set subfields, if defined
      maybeSearchSubFields.map {
        v => searchField.setFields(v: _*)
      }.getOrElse(searchField)
    } else if (dType.isComplex) {

      val inferredSearchType = inferSearchTypeFor(dType)
      if (inferredSearchType.isGeoPoint) {
        new SearchField(name, SearchFieldDataType.GEOGRAPHY_POINT)
      } else {
        val subFields = dType.unsafeSubFields.map(toSearchField)
        new SearchField(name, SearchFieldDataType.COMPLEX)
          .setFields(subFields: _*)
      }
    } else {
      throw DataTypeException.forUnsupportedSparkType(dType)
    }
  }
}
