package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.LexicalAnalyzerName
import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import com.github.jarol.azure.search.spark.sql.connector.core.config.{ConfigException, SearchConfig}

class AnalyzerConfigSpec
  extends BasicSpec {

  private lazy val alias = "first"
  private lazy val fieldList = Seq("hello", "world")
  private lazy val lexicalAnalyzerName = LexicalAnalyzerName.AR_LUCENE
  private lazy val analyzerType = SearchFieldAnalyzerType.INDEX_ANALYZER

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
        describe(s"${nameOf[NoSuchElementException]} for") {
          it("invalid lexical analyzers") {

            a [NoSuchElementException] shouldBe thrownBy {
              AnalyzerConfig.resolveLexicalAnalyzer("hello")
            }
          }

          it("invalid analyzer types") {

            a [NoSuchElementException] shouldBe thrownBy {
              AnalyzerConfig.resolveAnalyzerType("hello")
            }
          }
        }
      }

      describe("provide an empty instance when") {
        it("name is not defined") {

          AnalyzerConfig.createInstance(alias, emptyConfig) shouldBe empty
          AnalyzerConfig.createInstance(
            alias,
            createConfig(
              Map(
                WriteConfig.TYPE_SUFFIX -> analyzerType.name(),
                WriteConfig.ON_FIELDS_SUFFIX -> fieldList.mkString(",")
              )
            )
          ) shouldBe empty
        }

        it("type is not defined") {

          AnalyzerConfig.createInstance(alias, emptyConfig) shouldBe empty
          AnalyzerConfig.createInstance(
            alias,
            createConfig(
              Map(
                WriteConfig.NAME_SUFFIX -> lexicalAnalyzerName.toString,
                WriteConfig.ON_FIELDS_SUFFIX -> fieldList.mkString(",")
              )
            )
          ) shouldBe empty
        }

        it("field list is not defined") {

          AnalyzerConfig.createInstance(alias, emptyConfig) shouldBe empty
          AnalyzerConfig.createInstance(
            alias,
            createConfig(
              Map(
                WriteConfig.NAME_SUFFIX -> lexicalAnalyzerName.toString,
                WriteConfig.TYPE_SUFFIX -> analyzerType.name()
              )
            )
          ) shouldBe empty
        }
      }

      describe("provide a non-empty instance when") {
        it("all options are well-defined") {

          val maybeInstance = AnalyzerConfig.createInstance(
            alias,
            createConfig(
              Map(
                WriteConfig.NAME_SUFFIX -> lexicalAnalyzerName.toString,
                WriteConfig.TYPE_SUFFIX -> analyzerType.name(),
                WriteConfig.ON_FIELDS_SUFFIX -> fieldList.mkString(",")
              )
            )
          )

          maybeInstance shouldBe defined
          val instance = maybeInstance.get
          instance.name shouldBe lexicalAnalyzerName
          instance.`type` shouldBe analyzerType
          instance.fields should contain theSameElementsAs fieldList
        }
      }

      describe(s"throw a ${nameOf[ConfigException]} when") {
        it("an invalid lexical analyzer is given") {

          a [ConfigException] shouldBe thrownBy {

            AnalyzerConfig.createInstance(
              alias,
              createConfig(
                Map(
                  WriteConfig.NAME_SUFFIX -> "wrongAnalyzer",
                  WriteConfig.TYPE_SUFFIX -> analyzerType.name(),
                  WriteConfig.ON_FIELDS_SUFFIX -> fieldList.mkString(",")
                )
              )
            )
          }
        }

        it("an invalid type is given") {

          a [ConfigException] shouldBe thrownBy {

            AnalyzerConfig.createInstance(
              alias,
              createConfig(
                Map(
                  WriteConfig.NAME_SUFFIX -> lexicalAnalyzerName.toString,
                  WriteConfig.TYPE_SUFFIX -> "helloWorld",
                  WriteConfig.ON_FIELDS_SUFFIX -> fieldList.mkString(",")
                )
              )
            )
          }
        }
      }

      it("create a collection") {

        val (a1, a2) = ("a1", "a2")
        val expected: Map[String, (LexicalAnalyzerName, SearchFieldAnalyzerType, Seq[String])] = Map(
          a1 -> (LexicalAnalyzerName.BN_MICROSOFT, SearchFieldAnalyzerType.SEARCH_ANALYZER, Seq("first", "second")),
          a2 -> (LexicalAnalyzerName.STOP, SearchFieldAnalyzerType.INDEX_ANALYZER, Seq("third", "fourth"))
        )

        val rawConfig = expected.foldLeft(
          Map(WriteConfig.ALIASES_SUFFIX -> s"$a1,$a2")
        ) {
          case (seed, (alias, (name, analyzerType, fields))) =>
            seed ++ Map(
              s"$alias.${WriteConfig.NAME_SUFFIX}" -> name.toString,
              s"$alias.${WriteConfig.TYPE_SUFFIX}" -> analyzerType.name(),
              s"$alias.${WriteConfig.ON_FIELDS_SUFFIX}" -> fields.mkString(",")
            )
        }

        val maybeActual = AnalyzerConfig.createCollection(createConfig(rawConfig))
        maybeActual shouldBe defined
        val actual = maybeActual.get
        actual should have size expected.size
        forAll(actual) {
          analyzerConfig =>

            val maybeEntry = expected.get(analyzerConfig.alias)
            maybeEntry shouldBe defined
            val (_, analyzerType, fields) = maybeEntry.get
            analyzerConfig.`type` shouldBe analyzerType
            analyzerConfig.fields should contain theSameElementsAs fields
        }
      }
    }
  }

  describe(anInstanceOf[AnalyzerConfig]) {
    describe(SHOULD) {
      it("define a collection of actions") {

        val analyzer = LexicalAnalyzerName.BN_MICROSOFT
        val actions = AnalyzerConfig(alias, analyzer, SearchFieldAnalyzerType.SEARCH_ANALYZER, fieldList).actions
        actions should have size fieldList.size
        actions.map {
          case (k, _) => k
        } should contain theSameElementsAs fieldList
      }
    }
  }
}
