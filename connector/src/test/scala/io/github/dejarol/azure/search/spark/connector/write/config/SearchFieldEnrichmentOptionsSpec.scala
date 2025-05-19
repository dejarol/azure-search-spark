package io.github.dejarol.azure.search.spark.connector.write.config

import io.github.dejarol.azure.search.spark.connector.{BasicSpec, FieldFactory}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.types.DataTypes

class SearchFieldEnrichmentOptionsSpec
  extends BasicSpec
    with FieldFactory {

  /**
   * Creates an instance
   * @param options options
   * @param actionColumn action column
   * @return an option instance for testing
   */

  private def createOptions(
                             options: Option[Map[String, String]],
                             actionColumn: Option[String]
                           ): SearchFieldEnrichmentOptions = {

    SearchFieldEnrichmentOptions(
      CaseInsensitiveMap[String](options.getOrElse(Map.empty)),
      actionColumn
    )
  }

  describe(anInstanceOf[SearchFieldEnrichmentOptions]) {
    describe(SHOULD) {
      it("exclude index action column from a schema") {

        val actionCol = "action"
        val schema = Seq(
          createStructField(actionCol, DataTypes.StringType)
        )

        // Assert that the action column is excluded
        val optionsWithColumn = createOptions(None, Some(actionCol))
        optionsWithColumn.excludeIndexActionColumn(schema) shouldBe empty

        // Assert that the action column is not excluded
        val optionsWithoutColumn = createOptions(None, None)
        optionsWithoutColumn.excludeIndexActionColumn(schema) should contain theSameElementsAs schema
      }

      describe("collect actions only for") {
        it("valid field options") {

          val v3 =
            s"""
               |{
               |  "key": true
               |}
               |""".stripMargin

          val map = Map(
            "k1" -> "hello",
            "k2" -> "{}",
            "k3" -> v3
          )

          val fieldActions = createOptions(Some(map), None).fieldActions
          fieldActions should have size 1
          val maybeAction = fieldActions.get("k3")
          maybeAction shouldBe defined
        }
      }

      describe("convert a Spark schema to a collection of Search fields") {
        it("excluding the index action column") {

          val (actionCol, second) = ("first", "second")
          val options = createOptions(None, Some(actionCol))
          val searchFields = options.toSearchFields(
            Seq(
              createStructField(actionCol, DataTypes.StringType),
              createStructField(second, DataTypes.IntegerType)
            )
          )

          searchFields should have size 1
          searchFields.head.getName shouldBe second
        }

        it("applying defined enrichment options") {

          val (keyCol, second) = ("key", "second")
          val keyColValue =
            s"""
               |{
               |  "key": true
               |}
               |""".stripMargin

          val options = createOptions(
            Some(
              Map(
                keyCol -> keyColValue
              )
            ),
            None
          )

          val searchFields = options.toSearchFields(
            Seq(
              createStructField(keyCol, DataTypes.StringType),
              createStructField(second, DataTypes.IntegerType)
            )
          ).map {
            sf => (sf.getName, sf)
          }.toMap

          searchFields should have size 2
          val maybeKeyCol = searchFields.get(keyCol)
          maybeKeyCol shouldBe defined
          maybeKeyCol.get.isKey shouldBe true

          val maybeSecond = searchFields.get(second)
          maybeSecond shouldBe defined
          maybeSecond.get.isKey shouldBe null
        }
      }
    }
  }
}
