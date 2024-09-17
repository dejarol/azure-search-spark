package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.{BasicSpec, FieldFactory, JavaScalaConverters}
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.scalatest.Inspectors

class CreateSearchIndexOptionsSpec
  extends BasicSpec
    with FieldFactory
      with Inspectors {

  private lazy val (first, second, third) = ("first", "second", "third")
  private lazy val (firstField, secondField) = (
    createStructField(first, DataTypes.StringType),
    createStructField(second, DataTypes.IntegerType)
  )

  private lazy val schema = Seq(firstField, secondField)

  /**
   * Get a map with all Search index fields
   * @param options options for index creation
   * @param schema input schema
   * @return a map with keys being field names and values being field themselves
   */

  private def getSearchFields(
                               options: CreateSearchIndexOptions,
                               schema: Seq[StructField]
                             ): Map[String, SearchField] = {

    JavaScalaConverters.listToSeq(
      options.toSearchIndex(schema).getFields
    ).map {
      sf => (sf.getName, sf)
    }.toMap
  }

  describe(anInstanceOf[CreateSearchIndexOptions]) {
    describe(SHOULD) {
      it("not include index action column") {

        val indexActionColumn = third
        val schemaPlusActionColumn = schema :+ createStructField(indexActionColumn, DataTypes.StringType)
        val indexFields = getSearchFields(
          CreateSearchIndexOptions(
            "name",
            "key",
            None,
            None,
            None,
            None,
            None,
            Some(third)
          ),
          schemaPlusActionColumn
        )

        indexFields should not be empty
        val expectedKeys = schemaPlusActionColumn.collect {
          case sf if !sf.name.equalsIgnoreCase(indexActionColumn) => sf.name
        }
        indexFields shouldNot contain key indexActionColumn
        indexFields should have size expectedKeys.size
        forAll(expectedKeys) {
          k => indexFields should contain key k
        }
      }

      describe("set a field as") {
        it("filterable") {

          val filterableFields = Seq(first)
          val indexFields = getSearchFields(
            CreateSearchIndexOptions(
              "name",
              "key",
              Some(filterableFields),
              None,
              None,
              None,
              None,
              None
            ),
            schema
          )

          forAll(schema) {
            sf =>
              indexFields.get(sf.name).exists(_.isFilterable) shouldBe filterableFields.contains(sf.name)
          }
        }

        it("sortable") {

          val sortableFields = Seq(second)
          val indexFields = getSearchFields(
            CreateSearchIndexOptions(
              "name",
              "key",
              None,
              Some(sortableFields),
              None,
              None,
              None,
              None
            ),
            schema
          )

          forAll(schema) {
            sf =>
              indexFields.get(sf.name).exists(_.isSortable) shouldBe sortableFields.contains(sf.name)
          }
        }
      }
    }
  }
}
