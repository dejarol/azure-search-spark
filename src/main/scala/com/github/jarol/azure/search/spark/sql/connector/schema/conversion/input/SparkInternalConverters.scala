package com.github.jarol.azure.search.spark.sql.connector.schema.conversion.input

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.{AtomicTypeConversionRules, GeoPointRule}
import com.github.jarol.azure.search.spark.sql.connector.schema.{SchemaUtils, toSearchFieldOperations, toSearchTypeOperations}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

/**
 * Entry point for retrieving converters from Search objects to Spark types
 */

object SparkInternalConverters {

  /**
   * Safely retrieve the conversion to apply for reading data from a SearchField
   * <br>
   * The converter will exist if the two fields
   *  - have same name
   *  - have a compatible data type
   * @param structField Spark field
   * @param searchField Search field
   * @return an optional converter
   */

  def safeConverterFor(structField: StructField, searchField: SearchField): Option[SparkInternalConverter] = {

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

  private def converterForAtomicTypes(spark: DataType, search: SearchFieldDataType): Option[SparkInternalConverter] = {

    // For atomic types, there should exist either an inference rule or a conversion rule
    AtomicTypeConversionRules.safeSparkConverterForTypes(spark, search)
  }

  /**
   * Safely retrieve a converter for a collection type
   * <br>
   * A converter will exist only if the collection inner types are compatible
   * @param sparkType Spark array inner type
   * @param searchType Search collection inner type
   * @return a converter for collections
   */

  private def converterForArrayType(sparkType: DataType, searchType: SearchFieldDataType): Option[SparkInternalConverter] = {

    // Evaluate rule for the inner type
    val searchInnerType = searchType.unsafeCollectionInnerType
    sparkType match {
      case ArrayType(sparkInternalType, _) => safeConverterFor(
        StructField("array", sparkInternalType),
        new SearchField("array", searchInnerType)
      ).map(ArrayConverter)
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

  private def converterForComplexType(sparkType: DataType, searchField: SearchField): Option[SparkInternalConverter] = {

    // Build a rule that wraps subField conversion rules
    sparkType match {
      case StructType(sparkSubFields) =>

        // Compute the converter for to each subfield
        val searchSubFields = JavaScalaConverters.listToSeq(searchField.getFields)
        val convertersMap: Map[String, SparkInternalConverter] = SchemaUtils
          .matchNamesakeFields(sparkSubFields, searchSubFields)
          .map {
            case (k, v) => (k.name, safeConverterFor(k, v))
          }.collect {
            case (k, Some(v)) => (k, v)
          }

        // For each subfield, there should exist a search subfields with same name and a converter should exist
        val allSubFieldsExist = SchemaUtils.allSchemaFieldsExist(sparkSubFields, searchSubFields)
        val allSubFieldsHaveAConverter = sparkSubFields.forall {
          subField => convertersMap.exists {
            case (key, _) => key.equalsIgnoreCase(subField.name)
          }
        }

        // If so, create the Complex converter
        if (allSubFieldsExist && allSubFieldsHaveAConverter) {
          Some(ComplexConverter(convertersMap))
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

  private def converterForGeoPoint(sparkType: DataType): Option[SparkInternalConverter] = {

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
          Some(GeoPointRule.sparkConverter())
        } else {
          None
        }

      case _=> None
    }
  }
}
