package io.github.dejarol.azure.search.spark.connector.core.codec

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import io.github.dejarol.azure.search.spark.connector.core.schema._
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
 * Factory class to be extended for creating codecs
 * @param codecType instance of enum [[io.github.dejarol.azure.search.spark.connector.core.codec.CodecType]].
 *                  Should be <code>ENCODING</code> for encoding and <code>DECODING</code> for decoding
 * @tparam T codec type
 */

abstract class CodecFactory[T](protected val codecType: CodecType) {

  /**
   * Build a codec able to convert data from a [[org.apache.spark.sql.types.StructField]]
   * to a [[com.azure.search.documents.indexes.models.SearchField]] (or vice versa).
   * The result will be a left if the conversion is not possible, a right otherwise
   * @param structField a Spark field
   * @param searchField a Search field
   * @return either a codec or a [[CodecError]]
   */

  final def build(
                   structField: StructField,
                   searchField: SearchField
                 ): Either[CodecError, T] = {

    // Depending on Spark and Search types, detect the codec (if any)
    val (sparkType, searchFieldType) = (structField.dataType, searchField.getType)
    if (sparkType.isAtomic && searchFieldType.isAtomic) {
      // atomic types
      maybeAtomicCodec(sparkType, searchFieldType)
    } else if (sparkType.isCollection && searchFieldType.isCollection) {
      // array types
      maybeCodecForArrays(sparkType, searchField)
    } else if (sparkType.isComplex && searchFieldType.isComplex) {
      // complex types
      buildComplexCodecInternalMapping(sparkType.unsafeSubFields, searchField.unsafeSubFields)
        .right.map(createComplexCodec)
    } else if (sparkType.isComplex && searchFieldType.isGeoPoint) {
      // geo points
      maybeGeoPointCodec(structField)
    } else {
      Left(
        CodecErrors.forIncompatibleTypes(sparkType, searchFieldType)
      )
    }
  }

  /**
   * Retrieve the codec between two atomic types, eventually mapping a missing codec to a [[CodecError]]
   * @param sparkType Spark field
   * @param searchFieldType Search field
   * @return either a [[CodecError]] or an atomic codec
   */

  private def maybeAtomicCodec(
                                sparkType: DataType,
                                searchFieldType: SearchFieldDataType
                              ): Either[CodecError, T] = {

    atomicCodecFor(sparkType, searchFieldType) match {
      case Some(value) => Right(value)
      case None => Left(
        CodecErrors.forIncompatibleTypes(
          sparkType, searchFieldType
        )
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

  protected def atomicCodecFor(spark: DataType, search: SearchFieldDataType): Option[T]

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
                                 ): Either[CodecError, T] = {

    val (sparkInnerType, searchInnerType) = (
      sparkType.unsafeCollectionInnerType,
      searchField.getType.unsafeCollectionInnerType
    )

    // In inner type is complex, we have to bring in subFields definition from the wrapping Search field
    val searchArrayField = new SearchField("array", searchInnerType)
    val searchArrayFieldMaybeWithSubFields = searchField.safeSubFields match {
      case Some(value) => searchArrayField.setFields(value: _*)
      case None => searchArrayField
    }

    // Get the codec recursively
    build(
      StructField("array", sparkInnerType),
      searchArrayFieldMaybeWithSubFields
    ).right.map(
      collectionCodec(sparkInnerType, _)
    )
  }

  /**
   * Create a codec for data collection
   * @param internal internal codec (to use on collection inner objects)
   * @return a codec for collections
   */

  protected def collectionCodec(sparkType: DataType, internal: T): T


  /**
   * Safely retrieve the codec for a complex type
   * <br>
   * A codec will exist if and only if, for all Spark subfields, there exist a Search subField
   * with same name and compatible data type
   * @param sparkSubFields spark type
   * @param searchSubFields search field
   * @return a codec for complex fields
   */

  final def buildComplexCodecInternalMapping(
                                              sparkSubFields: Seq[StructField],
                                              searchSubFields: Seq[SearchField]
                                            ): Either[CodecError, Map[SearchIndexColumn, T]] = {

    // Create a case-insensitive map that collects Search fields
    // and link each Spark field to its homonymous Search field
    val searchIndexSchema = SearchIndexSchema(searchSubFields)
    val fieldPairs = sparkSubFields.map {
      sf => (sf, searchIndexSchema.get(sf.name))
    }

    // Isolate missing fields
    val missingSparkFields = fieldPairs.collect {
      case (field, None) => field.name
    }

    // If any, return a Left
    if (missingSparkFields.nonEmpty) {
      Left(
        makeExceptionForMissingFields(missingSparkFields)
      )
    } else {

      // Otherwise, try to build the codec for each subfield
      val structTypeOfSubfields = StructType(sparkSubFields)
      val subCodecs = fieldPairs.collect {
        case (sparkField, Some(searchField)) =>
          (
            SearchIndexColumnImpl(sparkField, structTypeOfSubfields),
            build(sparkField, searchField)
          )
      }.toMap

      // If all sub codecs are defined, we can build the complex codec
      val allInternalsAreDefined = subCodecs.values.forall(_.isRight)
      if (allInternalsAreDefined) {
        Right(
          subCodecs.collect {
            case (k, Right(v)) => (k, v)
          }
        )
      } else {

        // Otherwise, we should return a Left
        val failedSubCodecs = subCodecs.collect {
          case (k, Left(v)) => (k.name(), v)
        }

        Left(
          CodecErrors.forComplexObject(failedSubCodecs)
        )
      }
    }
  }

  /**
   * Create an exception for missing fields, depending on whether fieldName is defined.
   * If it's defined, it's supposed to be an exception related to a specific field.
   * @param missingFieldNames collection of missing field names
   * @return a [[CodecCreationException]]
   */

  private def makeExceptionForMissingFields(missingFieldNames: Seq[String]): CodecError = {

    CodecErrors.forComplexObject(
      missingFieldNames.map {
        name =>
          name -> CodecErrors.forMissingField()
      }.toMap
    )
  }

  /**
   * Create a codec for handling nested data objects
   * @param internal nested object mapping
   * @return a converter for handling nested data objects
   */

  protected def createComplexCodec(internal: Map[SearchIndexColumn, T]): T

  /**
   * Safely retrieve a codec for geopoints
   * <br>
   * A codec will exist if and only if given Spark types is compatible with the default geopoint schema
   * @param schemaField spark type
   * @return a converter for geo points
   */

  private def maybeGeoPointCodec(schemaField: StructField): Either[CodecError, T] = {

    // Evaluate if the field is eligible for being a GeoPoint
    val dataType = schemaField.dataType
    if (SchemaUtils.isEligibleAsGeoPoint(dataType)) {
      Right(
        forGeoPoint(
          StructType(
            dataType.unsafeSubFields
          )
        )
      )
    } else {
      Left(
        CodecErrors.notSuitableForGeoPoint(dataType)
      )
    }
  }

  /**
   * Get the codec for GeoPoints
   * @param schema schema of candidate field
   * @return converter for GeoPoints
   */

  protected def forGeoPoint(schema: StructType): T
}