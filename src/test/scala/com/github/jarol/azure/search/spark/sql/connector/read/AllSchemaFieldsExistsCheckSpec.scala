package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.{BasicSpec, SearchFieldFactory}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

class AllSchemaFieldsExistsCheckSpec
  extends BasicSpec
    with SearchFieldFactory {

  private lazy val (index, name, id) = ("people", "name", "id")
  private lazy val sparkStringField = StructField(name, DataTypes.StringType)
  private lazy val sparkIntField = StructField(id, DataTypes.IntegerType)
  private lazy val searchStringField = createSearchField(sparkStringField.name, SearchFieldDataType.STRING)

  /**
   * Create and execute a schema compatibility check, returning its optional exception
   * @param schema schema
   * @param searchFields search fields
   * @return the optional exception returned by this check
   */

  private def maybeExceptionFor(schema: Seq[StructField],
                                searchFields: Seq[SearchField]): Option[SchemaCompatibilityException] = {

    AllSchemaFieldsExistsCheck(
      StructType(schema),
      searchFields,
      index
    ).maybeException
  }

  describe(anInstanceOf[AllSchemaFieldsExistsCheck]) {
    describe(SHOULD) {
      describe("evaluate if all schema fields names exist on a Search index returning") {
        it("an empty Option for valid cases") {

          maybeExceptionFor(
            Seq(sparkStringField),
            Seq(searchStringField),
          ) shouldBe empty
        }

        it(s"a ${nameOf[SchemaCompatibilityException]} for invalid cases") {

          maybeExceptionFor(
            Seq(sparkStringField),
            Seq.empty
          ) shouldBe defined

          maybeExceptionFor(
            Seq(sparkStringField, sparkIntField),
            Seq(createSearchField(sparkStringField.name, SearchFieldDataType.STRING))
          ) shouldBe defined
        }
      }
    }
  }
}
