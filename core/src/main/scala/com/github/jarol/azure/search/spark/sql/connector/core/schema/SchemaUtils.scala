package com.github.jarol.azure.search.spark.sql.connector.core.schema

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{AtomicTypeConversionRules, GeoPointRule}
import com.github.jarol.azure.search.spark.sql.connector.core.{DataTypeException, JavaScalaConverters}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

import java.util

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
      AtomicTypeConversionRules.unsafeInferredTypeOf(searchType)
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
      GeoPointRule.sparkType
    } else {
      throw new DataTypeException(f"Unsupported Search type $searchType")
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
   * Evaluate the compatibility of a SearchField and a StructField. Those two entities are considered compatible if they have
   *  - same field name
   *  - compatible data type
   * @param structField struct field
   * @param searchField search field
   * @return true for compatible fields
   */

  final def areCompatibleFields(structField: StructField, searchField: SearchField): Boolean = {

    val (searchType, sparkType) = (searchField.getType, structField.dataType)
    val compatibleDataType: Boolean = if (searchType.isAtomic && sparkType.isAtomic) {

      // They should be either naturally compatible or a suitable conversion rule should exist
      evaluateSparkTypesCompatibility(inferSparkTypeOf(searchField), sparkType) ||
        AtomicTypeConversionRules.existsConversionRuleFor(sparkType, searchType)

    } else if (searchType.isCollection && sparkType.isCollection) {

      // Evaluate compatibility on inner types
      // A special case is represented by collection of complex objects
      val searchInnerType = searchType.unsafeCollectionInnerType
      val sparkInnerType = sparkType.unsafeCollectionInnerType

      // If both inner types are complex, evaluate subfields compatibility
      if (searchInnerType.isComplex && sparkInnerType.isComplex) {
        evaluateSubFieldsCompatibility(
          sparkInnerType.unsafeSubFields,
          searchField.getFields
        )
      } else {
        // Otherwise, evaluate standard compatibility
        areCompatibleFields(
          StructField("array", sparkInnerType),
          new SearchField("array", searchInnerType)
        )
      }

    } else if (searchType.isComplex && sparkType.isComplex) {
      // Evaluate compatibility on subfields
      evaluateSubFieldsCompatibility(
        sparkType.unsafeSubFields,
        searchField.getFields
      )
    } else if (searchType.isGeoPoint && sparkType.isComplex) {
      evaluateSparkTypesCompatibility(GeoPointRule.sparkType, sparkType)
    } else {
      false
    }

    searchField.sameNameOf(structField) && compatibleDataType
  }

  /**
   * Evaluate the compatibility of two sets of Spark and Search subfields
   * @param sparkSubFields Spark fields
   * @param searchSubFields Search fields
   * @return true for compatible set of fields
   */

  private def evaluateSubFieldsCompatibility(
                                              sparkSubFields: Seq[StructField],
                                              searchSubFields: util.List[SearchField]
                                            ): Boolean = {

    // Subfields are compatible if for each
    val searchSubFieldsSeq = JavaScalaConverters.listToSeq(searchSubFields)
    val allSubfieldsAreCompatible: Boolean = matchNamesakeFields(
      sparkSubFields,
      searchSubFieldsSeq
    ).forall {
      case (sparkSubField, searchSubField) =>
        areCompatibleFields(sparkSubField, searchSubField)
    }

    allSchemaFieldsExist(sparkSubFields, searchSubFieldsSeq) &&
      allSubfieldsAreCompatible
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
      AtomicTypeConversionRules.unsafeInferredTypeOf(dataType)
    } else if (dataType.isCollection) {
      SearchFieldDataType.collection(
        inferSearchTypeFor(dataType.unsafeCollectionInnerType)
      )
    } else if (dataType.isComplex) {

      // If compatible with GeoPoint, use Geography point Search data type
      val compatibleWithGeoPoint = evaluateSparkTypesCompatibility(dataType, GeoPointRule.GEO_POINT_DEFAULT_STRUCT)
      if (compatibleWithGeoPoint) {
        SearchFieldDataType.GEOGRAPHY_POINT
      } else SearchFieldDataType.COMPLEX
    } else {
      throw new DataTypeException(s"Unsupported Spark type ($dataType)")
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
      new SearchField(
        name,
        AtomicTypeConversionRules.unsafeInferredTypeOf(dType)
      )
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
      throw new DataTypeException(s"Unsupported Spark type ($dType)")
    }
  }
}
