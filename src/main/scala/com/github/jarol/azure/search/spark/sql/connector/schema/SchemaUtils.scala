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

  final def asStructType(fields: Seq[SearchField]): StructType = {

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
    val compatibleDataType: Boolean = if (searchType.isAtomic) {

      // They should be either naturally compatible or a suitable conversion rule should exist
      evaluateSparkTypesCompatibility(inferSparkTypeOf(searchField), sparkType) ||
        AtomicSchemaConversionRules.existsRuleFor(sparkType, searchType)
    } else if (searchType.isCollection) {

      // Evaluate compatibility on the inner type
      sparkType match {
        case ArrayType(elementType, _) => areCompatibleFields(
          StructField("array", elementType),
          new SearchField("array", searchType.unsafelyExtractCollectionType)
        )
        case _ => false
      }

    } else if (searchType.isComplex) {

      // Evaluate compatibility on subfields
      sparkType match {
        case StructType(sparkSubFields) =>

          val searchSubFields: Seq[SearchField] = JavaScalaConverters.listToSeq(searchField.getFields)

          // Subfields are compatible if for each
          val allSubfieldsAreCompatible: Boolean = matchNamesakeFields(sparkSubFields, searchSubFields).forall {
            case (sparkSubField, searchSubField) =>
              areCompatibleFields(sparkSubField, searchSubField)
          }

          allSchemaFieldsExist(sparkSubFields, searchSubFields) && allSubfieldsAreCompatible
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
