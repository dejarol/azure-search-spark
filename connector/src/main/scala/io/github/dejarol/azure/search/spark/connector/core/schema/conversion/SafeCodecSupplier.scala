package io.github.dejarol.azure.search.spark.connector.core.schema.conversion

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters
import io.github.dejarol.azure.search.spark.connector.core.schema._
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import java.util.{List => JList}

/**
 * Safe codec supplier.
 * <br>
 * Used for retrieving encoding or decoding schemas from Spark to Search types and vice versa
 * @tparam CType codec type
 */

trait SafeCodecSupplier[CType] {

  /**
   * Safely get the codec between a Spark schema and a Search index schema.
   * <br>
   * It returns either a collection of [[io.github.dejarol.azure.search.spark.connector.core.schema.conversion.SchemaViolation]]
   * (reporting fields that are not codecable) or the codec mapping (a map with keys being [[SearchIndexColumn]]
   * and values being instances of this instance's type
 *
   * @param schema Spark schema fields
   * @param searchFields Search index fields
   * @return either some schema violations or the codec function
   */

  final def get(
                 schema: StructType,
                 searchFields: Seq[SearchField]
               ): Either[Seq[SchemaViolation], Map[SearchIndexColumn, CType]] = {

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
                                       schema: StructType,
                                       searchFields: Seq[SearchField]
                                     ): Map[SearchIndexColumn, Either[SchemaViolation, CType]] = {

    schema.map {
      schemaField =>
        (
          // Create the Search index column
          SearchIndexColumnImpl(schemaField, schema),
          // Collect namesake field
          searchFields.collectFirst {
            case sef if sef.getName.equalsIgnoreCase(schemaField.name) => sef
          }.toRight(schemaField).left.map {
            // Map missing fields to a SchemaViolation
            sf => SchemaViolations.forMissingField(sf)
          }.flatMap {
            // Otherwise, get the codec
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
                         ): Either[SchemaViolation, CType] = {

    // Depending on Spark and Search types, detect the converter (if any)
    val (sparkType, searchFieldType) = (schemaField.dataType, searchField.getType)
    if (sparkType.isAtomic && searchField.isAtomic) {
      // atomic types
      maybeAtomicCodec(schemaField, searchField)
    } else if (sparkType.isCollection && searchField.isCollection) {
      // array types
      maybeCodecForArrays(sparkType, searchField)
    } else if (sparkType.isComplex && searchField.isComplex) {
      // complex types
      maybeCodecForComplex(schemaField, searchField)
    } else if (sparkType.isComplex && searchField.isGeoPoint) {
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
                              ): Either[SchemaViolation, CType] = {

    val (sparkType, searchFieldType) = (schemaField.dataType, searchField.getType)
    atomicCodecFor(sparkType, searchFieldType)
      .toRight(()).left.map {
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

  protected def atomicCodecFor(spark: DataType, search: SearchFieldDataType): Option[CType]

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
                                 ): Either[SchemaViolation, CType] = {

    val (sparkInnerType, searchInnerType) = (
      sparkType.unsafeCollectionInnerType,
      searchField.unsafeCollectionInnerType
    )

    // In inner type is complex, we have to bring in subFields definition from the wrapping Search field
    val maybeSubFields: Option[JList[SearchField]] = if (searchInnerType.equals(SearchFieldDataType.COMPLEX)) {
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
                                  ): Either[SchemaViolation, CType] = {

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

  protected def collectionCodec(sparkType: DataType, internal: CType): CType

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
                               ): Either[Seq[SchemaViolation], CType] = {

    // Build a rule that wraps subField conversion rules
    val maybeSubfieldMapping = maybeComplexObjectCodec(
      StructType(sparkType.unsafeSubFields),
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

  protected def createComplexCodec(internal: Map[SearchIndexColumn, CType]): CType

  /**
   * Safely retrieve a codec for geopoints
   * <br>
   * A codec will exist if and only if given Spark types is compatible with the default geopoint schema
   * (look at [[GeoPointType.SCHEMA]])
   *
   * @param schemaField spark type
   * @return a converter for geo points
   */

  private def maybeGeoPointCodec(schemaField: StructField): Either[SchemaViolation, CType] = {

    // Evaluate if the field is eligible for being a GeoPoint
    val dataType = schemaField.dataType
    val allSubFieldsExistAndAreCompatible = SchemaUtils.isEligibleAsGeoPoint(dataType)
    (if (allSubFieldsExistAndAreCompatible) {
      Some(
        forGeoPoint(StructType(dataType.unsafeSubFields))
      )
    } else {
      None
    }).toRight(()).left.map {
      _ => SchemaViolations.forIncompatibleGeoPoint(schemaField)
    }
  }

  /**
   * Get the codec for GeoPoints
   * @param schema schema of candidate field
   * @return converter for GeoPoints
   */

  protected def forGeoPoint(schema: StructType): CType
}
