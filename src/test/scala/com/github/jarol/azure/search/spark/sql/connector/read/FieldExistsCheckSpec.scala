package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.{BasicSpec, SearchFieldFactory, StructFieldFactory}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

class FieldExistsCheckSpec
  extends BasicSpec
    with SearchFieldFactory
      with StructFieldFactory {

  private lazy val (index, name, id) = ("people", "name", "id")
  private lazy val sparkStringField = createStructField(name, DataTypes.StringType)
  private lazy val sparkIntField = createStructField(id, DataTypes.IntegerType)
  private lazy val searchStringField = createSearchField(sparkStringField.name, SearchFieldDataType.STRING)

  /**
   * Create and execute a schema compatibility check, returning its optional exception
   * @param schema schema
   * @param searchFields search fields
   * @return the optional exception returned by this check
   */

  private def maybeExceptionFor(schema: Seq[StructField],
                                searchFields: Seq[SearchField]): Option[SchemaCompatibilityException] = {

    FieldExistsCheck(
      StructType(schema),
      searchFields,
      index
    ).maybeException
  }

  describe(anInstanceOf[FieldExistsCheck]) {
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
