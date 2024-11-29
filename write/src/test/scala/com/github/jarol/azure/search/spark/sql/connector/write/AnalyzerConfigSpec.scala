package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.LexicalAnalyzerName
import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import com.github.jarol.azure.search.spark.sql.connector.core.config.{ConfigException, SearchConfig}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.{SearchFieldAction, SearchFieldActions}

class AnalyzerConfigSpec
  extends BasicSpec {

  private lazy val firstAlias = "first"
  private lazy val fieldList = Seq("hello", "world")
  private lazy val analyzer = LexicalAnalyzerName.AR_LUCENE
  private lazy val action: LexicalAnalyzerName => SearchFieldAction = SearchFieldActions.forSettingAnalyzer

  /**
   * Create a [[SearchConfig]] instance
   * @param options options
   * @return a config instance
   */

  private def createConfig(options: Map[String, String]): SearchConfig = new SearchConfig(options)

  private lazy val emptyConfig = createConfig(Map.empty)

  describe(`object`[AnalyzerConfig]) {
    describe(SHOULD) {
      describe(s"throw a") {
        it(s"${nameOf[NoSuchElementException]} for invalid analyzer names") {

          a [NoSuchElementException] shouldBe thrownBy {
            AnalyzerConfig.resolveAnalyzer("hello")
          }
        }
      }

      describe("provide an empty instance when") {
        it("type is not defined") {

          AnalyzerConfig.createInstance(firstAlias, emptyConfig, action) shouldBe empty
          AnalyzerConfig.createInstance(
            firstAlias,
            createConfig(
              Map(s"$firstAlias.${WriteConfig.ON_FIELDS_SUFFIX}" -> fieldList.mkString(","))
            ),
            action
          ) shouldBe empty
        }

        it("field list is not defined") {

          AnalyzerConfig.createInstance(firstAlias, emptyConfig, action) shouldBe empty
          AnalyzerConfig.createInstance(
            firstAlias,
            createConfig(
              Map(s"$firstAlias.${WriteConfig.TYPE_SUFFIX}" -> analyzer.toString)
            ),
            action
          ) shouldBe empty
        }
      }

      describe("provide a non-empty instance when") {
        it("all options are defined") {

          val maybeInstance = AnalyzerConfig.createInstance(
            firstAlias,
            createConfig(
              Map(
                s"$firstAlias.${WriteConfig.TYPE_SUFFIX}" -> analyzer.toString,
                s"$firstAlias.${WriteConfig.ON_FIELDS_SUFFIX}" -> fieldList.mkString(",")
              )
            ),
            action
          )

          maybeInstance shouldBe defined
          val instance = maybeInstance.get
          instance.name shouldBe analyzer
          instance.fields should contain theSameElementsAs fieldList
        }
      }

      describe(s"throw a ${nameOf[ConfigException]} when") {
        it("an invalid analyzer is given") {

          a [ConfigException] shouldBe thrownBy {

            AnalyzerConfig.createInstance(
              firstAlias,
              createConfig(
                Map(
                  s"$firstAlias.${WriteConfig.TYPE_SUFFIX}" -> "wrongAnalyzer",
                  s"$firstAlias.${WriteConfig.ON_FIELDS_SUFFIX}" -> fieldList.mkString(",")
                )
              ),
              action
            )
          }
        }
      }
    }
  }

  describe(anInstanceOf[AnalyzerConfig]) {
    describe(SHOULD) {
      it("define a collection of actions") {

        val analyzer = LexicalAnalyzerName.BN_MICROSOFT
        val actions = AnalyzerConfig(analyzer, fieldList, action).actions
        actions should have size fieldList.size
      }
    }
  }
}
