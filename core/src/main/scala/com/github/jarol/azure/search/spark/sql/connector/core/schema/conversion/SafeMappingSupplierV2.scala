package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.core.schema._
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.MappingViolations._
import org.apache.spark.sql.types.{DataType, StructField}

import java.util

trait SafeMappingSupplierV2[A] {

  private type TOutput = Either[Seq[MappingViolation], Map[FieldAdapter, A]]

  def build(
             schema: Seq[StructField],
             searchFields: Seq[SearchField]
           ): TOutput = {

    val eitherViolationOrConverter = maybeComplexObjectMapping(schema, searchFields, None)
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
                                         searchFields: Seq[SearchField],
                                         prefix: Option[String]
                                       ): Map[FieldAdapter, Either[MappingViolation, A]] = {

    schema.map {
      schemaField =>
        (
          new FieldAdapter(schemaField),
          searchFields.collectFirst {
            case sef if sef.sameNameOf(schemaField) => sef
          }.toRight(schemaField).left.map {
            sf => new MissingField(sf, prefix.orNull)
          }.flatMap {
            searchField =>
              getConverterFor(schemaField, searchField, prefix)
          }
        )
    }.toMap
  }

  private def getConverterFor(
                               schemaField: StructField,
                               searchField: SearchField,
                               prefix: Option[String]
                             ): Either[MappingViolation, A] = {

    // Depending on Spark and Search types, detect the converter (if any)
    val (sparkType, searchFieldType) = (schemaField.dataType, searchField.getType)
    if (sparkType.isAtomic && searchFieldType.isAtomic) {
      // atomic types
      eitherViolationOrMappingForAtomic(schemaField, searchField, prefix)
    } else if (sparkType.isCollection && searchFieldType.isCollection) {
      // array types
      eitherViolationOrConverterForArrays(sparkType, searchField, prefix)
    } else if (sparkType.isComplex && searchFieldType.isComplex) {
      // complex types
      eitherViolationOrConverterForComplex(schemaField, searchField, prefix)
    } else if (sparkType.isComplex && searchFieldType.isGeoPoint) {
      // geo points
      eitherViolationOrConverterForGeoPoints(schemaField, prefix)
    } else {
      Left(
        new IncompatibleType(schemaField, searchField, prefix.orNull)
      )
    }
  }

  private def eitherViolationOrMappingForAtomic(
                                                 schemaField: StructField,
                                                 searchField: SearchField,
                                                 prefix: Option[String]
                                               ): Either[MappingViolation, A] = {

    val (sparkType, searchFieldType) = (schemaField.dataType, searchField.getType)
    forAtomicTypes(sparkType, searchFieldType)
      .toRight().left.map {
        _ => new IncompatibleType(schemaField, searchField, prefix.orNull)
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
                                     searchField: SearchField,
                                     prefix: Option[String]
                                   ): Either[MappingViolation, A] = {

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
    ).left.map {
      new ArrayViolation(searchField, _, prefix.orNull)
    }.map(
      forCollection(sparkInnerType, searchField, _)
    )
  }

  private def eitherViolationOrConverterForComplex(
                                                    schemaField: StructField,
                                                    searchField: SearchField,
                                                    prefix: Option[String]
                                                  ): Either[IncompatibleNestedField, A] = {

    converterForComplexType(
      schemaField.dataType,
      searchField,
      prefix.map(_.concat(searchField.getName))
    ).left.map {
      v => new IncompatibleNestedField(
        schemaField,
        JavaScalaConverters.seqToList(v),
        prefix.orNull
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
                                       searchField: SearchField,
                                       prefix: Option[String]
                                     ): Either[Seq[MappingViolation], A] = {

    // Build a rule that wraps subField conversion rules
    val maybeSubfieldMapping = maybeComplexObjectMapping(
      sparkType.unsafeSubFields,
      JavaScalaConverters.listToSeq(searchField.getFields),
      prefix
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

      Right(
        forComplex(internal)
      )
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
   * (look at [[GeoPointRule.GEO_POINT_DEFAULT_STRUCT]])
   * @param schemaField spark type
   * @return a converter for geo points
   */

  private def eitherViolationOrConverterForGeoPoints(
                                                      schemaField: StructField,
                                                      prefix: Option[String]
                                                    ): Either[MappingViolation, A] = {

    val allSubFieldsExist = schemaField.dataType.unsafeSubFields.forall {
      sf => GeoPointRule.GEO_POINT_DEFAULT_STRUCT.exists {
        geoSf => geoSf.name.equals(sf.name) && SchemaUtils.evaluateSparkTypesCompatibility(
          sf.dataType,
          geoSf.dataType
        )
      }
    }

    (if (allSubFieldsExist) {
      Some(forGeoPoint)
    } else {
      None
    }).toRight().left.map {
      _ => new NotSuitableAsGeoPoint(schemaField, prefix.orNull)
    }
  }

  /**
   * Converter for GeoPoints
   * @return converter for GeoPoints
   */

  protected def forGeoPoint: A
}
