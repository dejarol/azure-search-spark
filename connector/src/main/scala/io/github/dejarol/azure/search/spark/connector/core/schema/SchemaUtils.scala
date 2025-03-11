package io.github.dejarol.azure.search.spark.connector.core.schema

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import io.github.dejarol.azure.search.spark.connector.core.{DataTypeException, JavaScalaConverters}
import io.github.dejarol.azure.search.spark.connector.core.schema.conversion.GeoPointType
import org.apache.spark.sql.types._

import java.util.{List => JList}

/**
 * Utilities for dealing with both Spark and Search types
 */

object SchemaUtils {

  /**
   * Return the Spark equivalent [[org.apache.spark.sql.types.DataType]] for a search field
   * @param searchField a search field
   * @throws io.github.dejarol.azure.search.spark.connector.core.DataTypeException if given search type is not supported
   * @return the equivalent Spark data type for given search field
   */

  @throws[DataTypeException]
  final def inferSparkTypeOf(searchField: SearchField): DataType = {

    val searchType = searchField.getType
    if (searchType.isAtomic) {
      inferSparkAtomicType(searchType)
    } else if (searchType.isCollection) {

      // Extract collection inner type
      val innerSearchType = searchType.unsafeCollectionInnerType
      val innerSparkType = innerSearchType match {
        case SearchFieldDataType.SINGLE => DataTypes.FloatType
        case SearchFieldDataType.COMPLEX => inferSparkComplexType(searchField.getFields)
        case _ => inferSparkTypeOf(
          new SearchField("array", innerSearchType)
        )
      }

      ArrayType(innerSparkType, containsNull = true)
    } else if (searchType.isComplex) {
      inferSparkComplexType(searchField.getFields)
    } else if (searchType.isGeoPoint) {
      GeoPointType.SCHEMA
    } else {
      throw DataTypeException.forUnsupportedSearchType(searchType)
    }
  }

  /**
   * Infer the Spark type for an atomic Search type
   * @param searchType search type
   * @throws io.github.dejarol.azure.search.spark.connector.core.DataTypeException for unsupported Search types
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
        case SearchFieldDataType.SINGLE => throw DataTypeException.forSingleSearchFieldDataType()
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
   * Infer the Spark type for a collection on Search subfields
   * @param subFields Search subfields
   * @return a StructType wrapping all subFields definitions
   */

  private def inferSparkComplexType(subFields: JList[SearchField]): StructType = {

    // Create a StructType wrapping all subfields
    val seqOfSubFields = JavaScalaConverters.listToSeq(subFields).map {
      searchField =>
        StructField(
          searchField.getName,
          inferSparkTypeOf(searchField)
        )
    }

    StructType(seqOfSubFields)
  }

  /**
   * Convert a search field to a [[org.apache.spark.sql.types.StructField]]
   * @param searchField search field
   * @return the equivalent [[org.apache.spark.sql.types.StructField]] of this search field
   */

  protected[schema] def toStructField(searchField: SearchField): StructField = {

    StructField(
      searchField.getName,
      inferSparkTypeOf(searchField)
    )
  }

  /**
   * Convert a SearchIndex schema to a [[org.apache.spark.sql.types.StructType]]
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
      case (f: StructType, s: StructType) => f.size.equals(s.size) && f.forall {
        ff => s.exists {
          ss => ss.name.equals(ff.name) &&
            evaluateSparkTypesCompatibility(ff.dataType, ss.dataType)
        }
      }
      case _ => first.equals(second)
    }
  }

  /**
   * Infer the [[com.azure.search.documents.indexes.models.SearchFieldDataType]] for a Spark type
   * @param dataType Spark type
   * @throws io.github.dejarol.azure.search.spark.connector.core.DataTypeException for unsupported data types
   * @return the inferred Search type
   */

  @throws[DataTypeException]
  final def inferSearchTypeFor(dataType: DataType): SearchFieldDataType = {

    if (dataType.isAtomic) {
      inferSearchAtomicType(dataType)
    } else if (dataType.isCollection) {
      val innerDType = dataType.unsafeCollectionInnerType
      val innerSearchType = innerDType match {
        case DataTypes.FloatType => SearchFieldDataType.SINGLE
        case _ => inferSearchTypeFor(innerDType)
      }

      SearchFieldDataType.collection(innerSearchType)
    } else if (dataType.isComplex) {

      // If compatible with GeoPoint, use Geography point Search data type
      if (isEligibleAsGeoPoint(dataType)) {
        SearchFieldDataType.GEOGRAPHY_POINT
      } else SearchFieldDataType.COMPLEX
    } else {
      throw DataTypeException.forUnsupportedSparkType(dataType)
    }
  }

  /**
   * Evaluate if a data type is eligible for being a Search GeoPoint type
   * <br>
   * An eligible Spark type is a [[org.apache.spark.sql.types.StructType]] with 2 inner fields
   *  - <b>type</b> (String)
   *  - <b>coordinates</b> (Array(Double))
   * @param dataType Spark type
   * @return true for eligible types
   */

  final def isEligibleAsGeoPoint(dataType: DataType): Boolean = {

    evaluateSparkTypesCompatibility(
      dataType,
      GeoPointType.SCHEMA
    )
  }

  /**
   * Infer the Search type for an atomic Spark type
   * @param dataType Spark type
   * @throws io.github.dejarol.azure.search.spark.connector.core.DataTypeException for unsupported data types
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
   * Convert a Spark field into a Search field, by inferring the equivalent Search data type
   * and setting all the required field properties
   * @param structField Spark field.
   * @param fieldActions map with keys being field paths and values being a collection of actions to apply for such field
   * @param parentPath parent path for the structField (empty for top-level fields)
   * @throws io.github.dejarol.azure.search.spark.connector.core.DataTypeException for Spark fields with unsupported types
   * @return the equivalent Search field
   */

  @throws[DataTypeException]
  final def toSearchField(
                           structField: StructField,
                           fieldActions: Map[String, Seq[SearchFieldAction]],
                           parentPath: Option[String]
                         ): SearchField = {

    val (name, dType) = (structField.name, structField.dataType)
    val currentPath: String = parentPath match {
      case Some(value) => s"$value.$name"
      case None => name
    }

    val searchField: SearchField = if (dType.isAtomic) {
      new SearchField(name, inferSearchTypeFor(dType))
    } else if (dType.isCollection) {

      val searchField = new SearchField(name, inferSearchTypeFor(dType))

      // If the inner type is complex but not GeoPoint, we should add subFields to newly created field
      val innerDType = dType.unsafeCollectionInnerType
      val notEligibleAsGeoPoint = !SchemaUtils.isEligibleAsGeoPoint(innerDType)
      if (innerDType.isComplex && notEligibleAsGeoPoint) {
        val subFields: Seq[SearchField] = innerDType.unsafeSubFields.map {
          toSearchField(_, fieldActions, Some(currentPath))
        }
        searchField.setFields(subFields: _*)
      } else {
        searchField
      }
    } else if (dType.isComplex) {

      val inferredSearchType = inferSearchTypeFor(dType)
      if (inferredSearchType.isGeoPoint) {
        new SearchField(name, SearchFieldDataType.GEOGRAPHY_POINT)
      } else {
        val subFields = dType.unsafeSubFields.map {
          toSearchField(_, fieldActions, Some(currentPath))
        }
        new SearchField(name, SearchFieldDataType.COMPLEX)
          .setFields(subFields: _*)
      }
    } else {
      throw DataTypeException.forUnsupportedSparkType(dType)
    }

    // Retrieve available actions, apply them if necessary
    fieldActions.collectFirst {
      case (k, v) if k.equalsIgnoreCase(currentPath) => v
    } match {
      case Some(value) => searchField.applyActions(value: _*)
      case None => searchField
    }
  }
}
