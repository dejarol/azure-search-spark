package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.{BasicSpec, SearchFieldFactory, StructFieldFactory}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField}

class CompatibleTypesCheckSpec
  extends BasicSpec
    with SearchFieldFactory
      with StructFieldFactory {

  /**
   * Evaluate compatibility between a search field and Spark type
   * @param searchField search field
   * @param spark Spark type
   * @return true if compatible
   */

  private def areCompatible(searchField: SearchField, spark: DataType): Boolean = {

    CompatibleTypesCheck.areCompatible(
      searchField,
      createStructField("f2", spark)
    )
  }

  /**
   * Evaluate compatibility between a search type and Spark type
   * @param searchType search type
   * @param spark Spark type
   * @return true for compatible types
   */

  private def areCompatible(searchType: SearchFieldDataType, spark: DataType): Boolean = {

    CompatibleTypesCheck.areCompatible(
      createSearchField("f1", searchType),
      createStructField("f2", spark)
    )
  }

  /**
   * Execute the compatibility check and return its optional exception
   * @param sparkFields spark fields
   * @param searchFields search fields
   * @return the optional exception returned by the compatibility check
   */

  private def maybeExceptionFor(sparkFields: Seq[StructField], searchFields: Seq[SearchField]): Option[SchemaCompatibilityException] = {

    CompatibleTypesCheck(
      createStructType(sparkFields: _*),
      searchFields,
      "people"
    ).maybeException
  }

  describe(`object`[CompatibleTypesCheck]) {
    describe(SHOULD) {
      describe("evaluate type compatibility") {
        it("for atomic types") {

          areCompatible(SearchFieldDataType.STRING, DataTypes.StringType) shouldBe true
          areCompatible(SearchFieldDataType.DATE_TIME_OFFSET, DataTypes.TimestampType) shouldBe true
          areCompatible(SearchFieldDataType.BOOLEAN, DataTypes.LongType) shouldBe false

          // Schema conversion rule should hold
          areCompatible(SearchFieldDataType.DATE_TIME_OFFSET, DataTypes.DateType) shouldBe true
        }

        it("for array types") {

          areCompatible(
            createCollectionType(SearchFieldDataType.INT32),
            createArrayType(DataTypes.IntegerType)
          ) shouldBe true

          areCompatible(
            createCollectionType(SearchFieldDataType.STRING),
            createArrayType(DataTypes.BooleanType)
          ) shouldBe false
        }

        it("for complex types") {

          areCompatible(
            createComplexField(
              "name",
              Seq(
                createSearchField("string", SearchFieldDataType.STRING),
                createSearchField("int", SearchFieldDataType.INT32),
                createSearchField("date", SearchFieldDataType.DATE_TIME_OFFSET)
              )
            ),
            createStructType(
              createStructField("string", DataTypes.StringType),
              createStructField("int", DataTypes.IntegerType),
              createStructField("date", DataTypes.DateType)
            )
          ) shouldBe true
        }
      }
    }
  }

  describe(anInstanceOf[CompatibleTypesCheck]) {
    describe(SHOULD) {
      describe("evaluate data types incompatibilities returning") {
        it("an empty Option for valid cases") {

          maybeExceptionFor(
            Seq(
              createStructField("string", DataTypes.StringType),
              createArrayField("array", DataTypes.IntegerType)
            ),
            Seq(
              createSearchField("string", SearchFieldDataType.STRING),
              createSearchField("array", createCollectionType(SearchFieldDataType.INT32))
            )
          ) shouldBe empty
        }

        it(s"a ${nameOf[SchemaCompatibilityException]} otherwise") {

          maybeExceptionFor(
            Seq(
              createStructField("string", DataTypes.StringType),
              createArrayField("array", DataTypes.IntegerType)
            ),
            Seq(
              createSearchField("string", SearchFieldDataType.STRING),
              createSearchField("array", createCollectionType(SearchFieldDataType.BOOLEAN))
            )
          ) shouldBe defined
        }
      }
    }
  }
}
