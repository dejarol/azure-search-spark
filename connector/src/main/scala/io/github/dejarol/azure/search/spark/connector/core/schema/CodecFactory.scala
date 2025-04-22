package io.github.dejarol.azure.search.spark.connector.core.schema

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import io.github.dejarol.azure.search.spark.connector.core.DataTypeException
import io.github.dejarol.azure.search.spark.connector.core.schema.conversion.{GeoPointType, SearchIndexColumn, SearchIndexColumnImpl}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import java.util.{List => JList}

trait CodecFactory[T] {

  protected[schema] def forEncoding: Boolean

  final def build(
                   structField: StructField,
                   searchField: SearchField
                 ): Either[Throwable, T] = {

    // Depending on Spark and Search types, detect the codec (if any)
    val (fieldName, sparkType, searchFieldType) = (structField.name, structField.dataType, searchField.getType)
    if (sparkType.isAtomic && searchFieldType.isAtomic) {
      // atomic types
      maybeAtomicCodec(sparkType, searchFieldType, fieldName)
    } else if (sparkType.isCollection && searchFieldType.isCollection) {
      // array types
      maybeCodecForArrays(sparkType, searchField, fieldName)
    } else if (sparkType.isComplex && searchFieldType.isComplex) {
      // complex types
      maybeCodecForComplex(sparkType.unsafeSubFields, searchField.unsafeSubFields, fieldName)
    } else if (sparkType.isComplex && searchFieldType.isGeoPoint) {
      // geo points
      maybeGeoPointCodec(structField)
    } else {
      Left(
        new DataTypeException("a")
      )
    }
  }

  /**
   * Retrieve the codec between two atomic types, eventually mapping a missing codec to a [[DataTypeException]]
   * @param sparkType Spark field
   * @param searchFieldType Search field
   * @return either a [[DataTypeException]] or an atomic codec
   */

  private def maybeAtomicCodec(
                                sparkType: DataType,
                                searchFieldType: SearchFieldDataType,
                                fieldName: String
                              ): Either[CodecFactoryException, T] = {

    atomicCodecFor(sparkType, searchFieldType) match {
      case Some(value) => Right(value)
      case None => Left(
        CodecFactoryException.forIncompatibleTypes(
          fieldName,
          sparkType,
          searchFieldType,
          forEncoding
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
                                   searchField: SearchField,
                                   fieldName: String
                                 ): Either[CodecFactoryException, T] = {

    val (sparkInnerType, searchInnerType) = (
      sparkType.unsafeCollectionInnerType,
      searchField.getType.unsafeCollectionInnerType
    )

    // In inner type is complex, we have to bring in subFields definition from the wrapping Search field
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
    build(
      StructField("array", sparkInnerType),
      searchArrayFieldMaybeWithSubFields
    ).left.map {
      cause => CodecFactoryException.forArrays(fieldName, cause, forEncoding)
    }.map(
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

  private def maybeCodecForComplex(
                                    sparkSubFields: Seq[StructField],
                                    searchSubFields: Seq[SearchField],
                                    fieldName: String
                                  ): Either[Throwable, T] = {

    // Create a case-insensitive map that collects Search fields
    // and link each Spark field to its homonymous Search field
    val searchIndexSchema = SearchIndexSchema(searchSubFields)
    val fieldPairs = sparkSubFields.map {
      sf => (sf, searchIndexSchema.get(sf.name))
    }

    // Isolate missing fields
    val missingSparkFields = fieldPairs.collect {
      case (field, None) => field
    }

    // If any, return a Left
    if (missingSparkFields.nonEmpty) {
      Left(
        CodecFactoryException.forComplex(fieldName, forEncoding)
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
          createComplexCodec(
            subCodecs.collect {
              case (k, Right(v)) => (k, v)
            }
          )
        )
      } else {

        // Otherwise, we should return a Left
        Left(
          CodecFactoryException.forComplex(fieldName, forEncoding)
        )
      }
    }
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
   * (look at [[GeoPointType.SCHEMA]])
   *
   * @param schemaField spark type
   * @return a converter for geo points
   */

  private def maybeGeoPointCodec(schemaField: StructField): Either[DataTypeException, T] = {

    // Evaluate if the field is eligible for being a GeoPoint
    val dataType = schemaField.dataType
    val allSubFieldsExistAndAreCompatible = SchemaUtils.isEligibleAsGeoPoint(dataType)
    if (allSubFieldsExistAndAreCompatible) {
      Right(
        forGeoPoint(StructType(dataType.unsafeSubFields))
      )
    } else {
      Left(
        new DataTypeException("b") // TODO: create ad-hoc constructor
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