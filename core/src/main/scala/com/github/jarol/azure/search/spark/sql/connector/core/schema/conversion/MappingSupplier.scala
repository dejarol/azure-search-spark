package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.core.schema._
import org.apache.spark.sql.types.{DataType, StructField}

import java.util

trait MappingSupplier[A] {

  def safelyGet(
                 schema: Seq[StructField],
                 searchFields: Seq[SearchField]
               ): Either[Seq[SchemaViolation], Map[FieldAdapter, A]] = {

    val eitherViolationOrConverter = maybeComplexObjectMapping(schema, searchFields)
    val violations = eitherViolationOrConverter.values.collect {
      case Left(v) => v
    }.toSeq

    if (violations.nonEmpty) {
      Left(violations)
    } else {

      Right(
        eitherViolationOrConverter.collect {
          case (k, Right(v)) => (k, v)
        }
      )
    }
  }

  private def maybeComplexObjectMapping(
                                         schema: Seq[StructField],
                                         searchFields: Seq[SearchField]
                                       ): Map[FieldAdapter, Either[SchemaViolation, A]] = {

    schema.map {
      schemaField =>
        (
          FieldAdapterImpl(schemaField),
          searchFields.collectFirst {
            case sef if sef.sameNameOf(schemaField) => sef
          }.toRight(schemaField).left.map {
            sf => SchemaViolations.forMissingField(sf)
          }.flatMap {
            searchField =>
              getConverterFor(schemaField, searchField)
          }
        )
    }.toMap
  }

  private def getConverterFor(
                               schemaField: StructField,
                               searchField: SearchField
                             ): Either[SchemaViolation, A] = {

    // Depending on Spark and Search types, detect the converter (if any)
    val (sparkType, searchFieldType) = (schemaField.dataType, searchField.getType)
    if (sparkType.isAtomic && searchFieldType.isAtomic) {
      // atomic types
      eitherViolationOrMappingForAtomic(schemaField, searchField)
    } else if (sparkType.isCollection && searchFieldType.isCollection) {
      // array types
      eitherViolationOrConverterForArrays(sparkType, searchField)
    } else if (sparkType.isComplex && searchFieldType.isComplex) {
      // complex types
      eitherViolationOrConverterForComplex(schemaField, searchField)
    } else if (sparkType.isComplex && searchFieldType.isGeoPoint) {
      // geo points
      eitherViolationOrConverterForGeoPoints(schemaField)
    } else {
      Left(
        SchemaViolations.forNamesakeButIncompatibleFields(schemaField.name, sparkType, searchFieldType)
      )
    }
  }

  private def eitherViolationOrMappingForAtomic(
                                                 schemaField: StructField,
                                                 searchField: SearchField
                                               ): Either[SchemaViolation, A] = {

    val (sparkType, searchFieldType) = (schemaField.dataType, searchField.getType)
    forAtomicTypes(sparkType, searchFieldType)
      .toRight().left.map {
        _ => SchemaViolations.forNamesakeButIncompatibleFields(schemaField.name, sparkType, searchFieldType)
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

  protected def forAtomicTypes(spark: DataType, search: SearchFieldDataType): Option[A]

  /**
   * Safely retrieve a converter for a collection type
   * <br>
   * A converter will exist only if the collection inner types are compatible
   * @param sparkType Spark array inner type
   * @param searchField Search collection inner type
   * @return a converter for collections
   */

  private def eitherViolationOrConverterForArrays(
                                                   sparkType: DataType,
                                                   searchField: SearchField
                                                 ): Either[SchemaViolation, A] = {

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
      searchArrayFieldMaybeWithSubFields
    ).left.map {
      SchemaViolations.forArrayField(searchField.getName, _)
    }.map(
      forCollection(sparkInnerType, searchField, _)
    )
  }

  private def eitherViolationOrConverterForComplex(
                                                    schemaField: StructField,
                                                    searchField: SearchField
                                                  ): Either[SchemaViolation, A] = {

    converterForComplexType(
      schemaField.dataType,
      searchField
    ).left.map {
      v => SchemaViolations.forComplexField(
        schemaField.name,
        JavaScalaConverters.seqToList(v)
      )
    }
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
                                       searchField: SearchField
                                     ): Either[Seq[SchemaViolation], A] = {

    // Build a rule that wraps subField conversion rules
    val maybeSubfieldMapping = maybeComplexObjectMapping(
      sparkType.unsafeSubFields,
      JavaScalaConverters.listToSeq(searchField.getFields)
    )

    val violations = maybeSubfieldMapping.values.collect {
      case Left(v) => v
    }.toSeq

    if (violations.nonEmpty) {
      Left(violations)
    } else {

      val internal = maybeSubfieldMapping.collect {
        case (k, Right(v)) => (k, v)
      }

      Right(forComplex(internal))
    }
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
   * (look at [[GeoPointType.SCHEMA]])
   *
   * @param schemaField spark type
   * @return a converter for geo points
   */

  private def eitherViolationOrConverterForGeoPoints(schemaField: StructField): Either[SchemaViolation, A] = {

    val allSubFieldsExistAndAreCompatible = schemaField.dataType.unsafeSubFields.forall {
      sf =>
        GeoPointType.SCHEMA.exists {
        geoSf => geoSf.name.equals(sf.name) && SchemaUtils.evaluateSparkTypesCompatibility(
          sf.dataType,
          geoSf.dataType
        )
      }
    }

    (if (allSubFieldsExistAndAreCompatible) {
      Some(forGeoPoint)
    } else {
      None
    }).toRight().left.map {
      _ => SchemaViolations.forIncompatibleGeoPoint(schemaField)
    }
  }

  /**
   * Converter for GeoPoints
   * @return converter for GeoPoints
   */

  protected def forGeoPoint: A
}
