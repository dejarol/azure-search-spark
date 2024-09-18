package com.github.jarol.azure.search.spark.sql.connector.schema.conversion.output

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.{AtomicTypeConversionRules, GeoPointRule}
import com.github.jarol.azure.search.spark.sql.connector.schema.{SchemaUtils, toSearchFieldOperations, toSearchTypeOperations}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

object SearchPropertyConverters {

  def safeConverterFor(structField: StructField, searchField: SearchField): Option[SearchPropertyConverter] = {

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

  private def converterForAtomicTypes(spark: DataType, search: SearchFieldDataType): Option[SearchPropertyConverter] = {

    AtomicTypeConversionRules.safeSearchConverterForTypes(spark, search)
  }

  private def converterForArrayType(sparkType: DataType, searchType: SearchFieldDataType): Option[SearchPropertyConverter] = {

    val searchInnerType = searchType.unsafeCollectionInnerType
    sparkType match {
      case ArrayType(elementType, _) => safeConverterFor(
        StructField("array", elementType),
        new SearchField("array", searchInnerType)
      ).map(CollectionConverter)

      case _ => None
    }
  }

  private def converterForComplexType(sparkType: DataType, searchField: SearchField): Option[SearchPropertyConverter] = {

    sparkType match {
      case StructType(sparkSubFields) =>
        // Compute the converter for to each subfield
        val searchSubFields = JavaScalaConverters.listToSeq(searchField.getFields)
        val convertersMap: Map[StructField, SearchPropertyConverter] = SchemaUtils
          .matchNamesakeFields(sparkSubFields, searchSubFields)
          .map {
            case (k, v) => (k, safeConverterFor(k, v))
          }.collect {
            case (k, Some(v)) => (k, v)
          }

        // For each subfield, there should exist a search subfields with same name and a converter should exist
        val allSubFieldsExist = SchemaUtils.allSchemaFieldsExist(sparkSubFields, searchSubFields)
        val allSubFieldsHaveAConverter = sparkSubFields.forall {
          subField => convertersMap.exists {
            case (key, _) => key.name.equalsIgnoreCase(subField.name)
          }
        }

        // If so, create the Complex converter
        if (allSubFieldsExist && allSubFieldsHaveAConverter) {
          Some(StructTypeConverter(convertersMap))
        } else {
          None
        }

      case _ => None
    }
  }

  private def converterForGeoPoint(sparkType: DataType): Option[SearchPropertyConverter] = {

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
          Some(GeoPointRule.searchConverter())
        } else {
          None
        }

      case _=> None
    }
  }
}
