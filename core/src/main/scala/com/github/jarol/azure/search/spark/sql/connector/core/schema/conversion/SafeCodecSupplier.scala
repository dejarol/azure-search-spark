package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.core.schema._
import org.apache.spark.sql.types.{DataType, StructField}

import java.util.{List => JList}

/**
 * Safe codec supplier.
 * <br>
 * Used for retrieving encoding or decoding schemas from Spark to Search types and vice versa
 * @tparam CodecType codec type
 */

trait SafeCodecSupplier[CodecType] {

  /**
   * Safely get the codec between a Spark schema and a Search index schema.
   * <br>
   * It returns either a collection of [[SchemaViolation]] (reporting fields that are not codecable) or
   * the codec mapping (a map with keys being [[FieldAdapter]] and values being instances of [[CodecType]])
   * @param schema Spark schema fields
   * @param searchFields Search index fields
   * @return either some schema violations or the codec function
   */

  def get(
           schema: Seq[StructField],
           searchFields: Seq[SearchField]
         ): Either[Seq[SchemaViolation], Map[FieldAdapter, CodecType]] = {

    val maybeCodec = maybeComplexObjectCodec(schema, searchFields)

    // Collect violations
    val violations = maybeCodec.values.collect {
      case Left(v) => v
    }.toSeq

    // If some, return a Left with all of them
    if (violations.nonEmpty) {
      Left(violations)
    } else {

      // Otherwise get the codec function
      Right(
        maybeCodec.collect {
          case (k, Right(v)) => (k, v)
        }
      )
    }
  }

  /**
   * Create a map with keys being field definitions and values being either a schema violation
   * (indicating a non-compatible field) or a codec for encoding/data from such field
   * @param schema Spark schema fields
   * @param searchFields Search index fields
   * @return a map with keys being field definitions and values being either a schema violation
   */

  private def maybeComplexObjectCodec(
                                       schema: Seq[StructField],
                                       searchFields: Seq[SearchField]
                                     ): Map[FieldAdapter, Either[SchemaViolation, CodecType]] = {

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
              getCodecFor(schemaField, searchField)
          }
        )
    }.toMap
  }

  /**
   * Retrieve the codec between two fields
   * @param schemaField Spark field
   * @param searchField Search fields
   * @return either a [[SchemaViolation]] or the codec
   */

  private def getCodecFor(
                           schemaField: StructField,
                           searchField: SearchField
                         ): Either[SchemaViolation, CodecType] = {

    // Depending on Spark and Search types, detect the converter (if any)
    val (sparkType, searchFieldType) = (schemaField.dataType, searchField.getType)
    if (sparkType.isAtomic && searchFieldType.isAtomic) {
      // atomic types
      maybeAtomicCodec(schemaField, searchField)
    } else if (sparkType.isCollection && searchFieldType.isCollection) {
      // array types
      maybeCodecForArrays(sparkType, searchField)
    } else if (sparkType.isComplex && searchFieldType.isComplex) {
      // complex types
      maybeCodecForComplex(schemaField, searchField)
    } else if (sparkType.isComplex && searchFieldType.isGeoPoint) {
      // geo points
      maybeGeoPointCodec(schemaField)
    } else {
      Left(
        SchemaViolations.forNamesakeButIncompatibleFields(
          schemaField.name,
          sparkType,
          searchFieldType
        )
      )
    }
  }

  /**
   * Retrieve the codec between two atomic types, eventually mapping a missing codec to a [[SchemaViolation]]
   * @param schemaField Spark field
   * @param searchField Search field
   * @return either a [[SchemaViolation]] or an atomic codec
   */

  private def maybeAtomicCodec(
                                schemaField: StructField,
                                searchField: SearchField
                              ): Either[SchemaViolation, CodecType] = {

    val (sparkType, searchFieldType) = (schemaField.dataType, searchField.getType)
    atomicCodecFor(sparkType, searchFieldType)
      .toRight().left.map {
        _ => SchemaViolations.forNamesakeButIncompatibleFields(
          schemaField.name,
          sparkType,
          searchFieldType
        )
      }
  }

  /**
   * Safely retrieve the codec between two atomic types
   * <br>
   * The codec will exist only for compatible atomic types
   * @param spark Spark type
   * @param search Search type
   * @return an optional codec for given types
   */

  protected def atomicCodecFor(spark: DataType, search: SearchFieldDataType): Option[CodecType]

  /**
   * Safely retrieve a codec for a collection type
   * <br>
   * A codec will exist only if the collection inner types are compatible
   * @param sparkType Spark array inner type
   * @param searchField Search collection inner type
   * @return a codec for collections
   */

  private def maybeCodecForArrays(
                                   sparkType: DataType,
                                   searchField: SearchField
                                 ): Either[SchemaViolation, CodecType] = {

    // In inner type is complex, we have to bring in subFields definition from the wrapping Search field
    val (sparkInnerType, searchInnerType) = (sparkType.unsafeCollectionInnerType, searchField.getType.unsafeCollectionInnerType)
    val maybeSubFields: Option[JList[SearchField]] = if (searchInnerType.isComplex) {
      Some(searchField.getFields)
    } else None

    // Bring subField in, if any
    val searchArrayField = new SearchField("array", searchInnerType)
    val searchArrayFieldMaybeWithSubFields = maybeSubFields match {
      case Some(value) => searchArrayField.setFields(value)
      case None => searchArrayField
    }

    // Get the converter recursively
    getCodecFor(
      StructField("array", sparkInnerType),
      searchArrayFieldMaybeWithSubFields
    ).left.map {
      SchemaViolations.forArrayField(searchField.getName, _)
    }.map(
      collectionCodec(sparkInnerType, _)
    )
  }

  /**
   * Retrieve the codec for a complex object, eventually wrapping collected violations into a new [[SchemaViolation]]
   * @param schemaField Spark field
   * @param searchField Search field
   * @return either a [[SchemaViolation]] containing all subFields violations, or the complex codec
   */

  private def maybeCodecForComplex(
                                    schemaField: StructField,
                                    searchField: SearchField
                                  ): Either[SchemaViolation, CodecType] = {

    maybeComplexCodec(
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
   * Create a codec for data collection
   * @param internal internal codec (to use on collection inner objects)
   * @return a codec for collections
   */

  protected def collectionCodec(sparkType: DataType, internal: CodecType): CodecType

  /**
   * Safely retrieve the codec for a complex type
   * <br>
   * A codec will exist if and only if, for all Spark subfields, there exist a Search subField
   * with same name and compatible data type
   * @param sparkType spark type
   * @param searchField search field
   * @return a codec for complex fields
   */

  private def maybeComplexCodec(
                                 sparkType: DataType,
                                 searchField: SearchField
                               ): Either[Seq[SchemaViolation], CodecType] = {

    // Build a rule that wraps subField conversion rules
    val maybeSubfieldMapping = maybeComplexObjectCodec(
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

      Right(createComplexCodec(internal))
    }
  }

  /**
   * Create a codec for handling nested data objects
   * @param internal nested object mapping
   * @return a converter for handling nested data objects
   */

  protected def createComplexCodec(internal: Map[FieldAdapter, CodecType]): CodecType

  /**
   * Safely retrieve a codec for geopoints
   * <br>
   * A codec will exist if and only if given Spark types is compatible with the default geopoint schema
   * (look at [[GeoPointType.SCHEMA]])
   *
   * @param schemaField spark type
   * @return a converter for geo points
   */

  private def maybeGeoPointCodec(schemaField: StructField): Either[SchemaViolation, CodecType] = {

    // Evaluate if the field is eligible for being a GeoPoint
    val allSubFieldsExistAndAreCompatible = schemaField.dataType.unsafeSubFields
      .forall { sf =>
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

  protected def forGeoPoint: CodecType
}
