package com.github.jarol.azure.search.spark.sql.connector.schema

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion._
import com.github.jarol.azure.search.spark.sql.connector.{AzureSparkException, JavaScalaConverters}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

/**
 * Utilities for
 *  - inferring a SearchField's datatype
 *  - convert a Search index schema to a Spark schema
 *  - evaluate Spark types compatibility
 *  - evaluate Spark field and Search field compatibility
 */

object SchemaUtils {

  /**
   * Return the Spark equivalent [[DataType]] for a search field
   * @param searchField a search field
   * @throws AzureSparkException if given search type is not supported
   * @return the equivalent Spark data type for given search field
   */

  @throws[AzureSparkException]
  final def inferSparkTypeOf(searchField: SearchField): DataType = {

    val searchType = searchField.getType
    if (searchType.isAtomic) {
      AtomicInferSchemaRules.unsafeInferredTypeOf(searchType)
    } else if (searchType.isCollection) {

      // Extract collection inner type
      ArrayType(
        inferSparkTypeOf(
          new SearchField("array", searchType.unsafelyExtractCollectionType)
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
      throw new AzureSparkException(f"Unsupported datatype $searchType")
    }
  }

  /**
   * Convert a search field to a [[StructField]]
   * @param searchField search field
   * @return the equivalent [[StructField]] of this search field
   */

  protected[schema] def asStructField(searchField: SearchField): StructField = {

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

  def asStructType(fields: Seq[SearchField]): StructType = {

    StructType(
      fields.map(asStructField)
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

  def evaluateSparkTypesCompatibility(first: DataType, second: DataType): Boolean = {

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
   * Evaluate the compatibility of a SearchField and a StructField.
   *
   * Those two entities are considered compatible if they have
   *  - same field name
   *  - compatible data type
   * @param structField struct field
   * @param searchField search field
   * @return true for compatible fields
   */

  def evaluateFieldsCompatibility(structField: StructField, searchField: SearchField): Boolean = {

    val (searchType, sparkType) = (searchField.getType, structField.dataType)
    val compatibleDataType: Boolean = if (searchType.isAtomic) {

      // They should be either naturally compatible or a suitable conversion rule should exist
      evaluateSparkTypesCompatibility(
        inferSparkTypeOf(searchField),
        sparkType
      ) || AtomicSchemaConversionRules.existsRuleFor(
        sparkType,
        searchType
      )
    } else if (searchType.isCollection) {

      // Evaluate compatibility on the inner type
      sparkType match {
        case ArrayType(elementType, _) => evaluateFieldsCompatibility(
          StructField("array", elementType),
          new SearchField("array", searchType.unsafelyExtractCollectionType)
        )
        case _ => false
      }

    } else if (searchType.isComplex) {

      // Evaluate compatibility for all subfields
      sparkType match {
        case StructType(sparkSubFields) =>

          val searchSubFields: Seq[SearchField] = JavaScalaConverters.listToSeq(searchField.getFields)

          // For each Spark subfield
          // [1] a Search subfield with same name should exist
          // [2] the Spark type and the Search type should be compatible
          val forAllSparkSubFieldsExistsASearchSubFieldWithSameName: Boolean = sparkSubFields.forall {
            spsField =>
              searchSubFields.exists {
                sesField => sesField.sameNameOf(spsField)
              }
          }

          val allSubfieldsAreCompatible: Boolean = sparkSubFields.map {
            spField =>
              // Collect first matching (i.e. with same name) field
              (spField, searchSubFields.collectFirst {
                case seField if seField.sameNameOf(spField) => seField
              })
          }.collect {
            // Collect cases where a matching field exists
            case (spField, Some(seField)) => (spField, seField)
          }.forall {
            case (sparkSubField, searchSubField) =>
              evaluateFieldsCompatibility(sparkSubField, searchSubField)
          }

          forAllSparkSubFieldsExistsASearchSubFieldWithSameName && allSubfieldsAreCompatible
        case _ => false
      }
    } else if (searchType.isGeoPoint) {
      evaluateSparkTypesCompatibility(
        inferSparkTypeOf(searchField),
        sparkType
      )
    } else {
      false
    }

    searchField.sameNameOf(structField) && compatibleDataType
  }
}
