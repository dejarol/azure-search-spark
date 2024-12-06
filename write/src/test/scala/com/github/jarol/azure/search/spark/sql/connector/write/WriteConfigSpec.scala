package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.{BM25SimilarityAlgorithm, ClassicTokenizer, LexicalAnalyzerName}
import com.azure.search.documents.models.IndexActionType
import com.github.jarol.azure.search.spark.sql.connector.core.config.ConfigException
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, SearchAPIModelFactory}

class WriteConfigSpec
  extends BasicSpec
    with WriteConfigFactory
      with SearchAPIModelFactory {

  private lazy val emptyConfig: WriteConfig = WriteConfig(Map.empty[String, String])
  private lazy val (keyField, indexActionColumn) = ("hello", "world")

  /**
   * Assert that an optional collection of strings is defined and contains the same elements w.r.t an expected set
   * @param actual actual set (optional)
   * @param expected expected set
   */

  private def assertDefinedAndContaining(
                                          actual: Option[Seq[String]],
                                          expected: Seq[String]
                                        ): Unit = {

    actual shouldBe defined
    actual.get should contain theSameElementsAs expected
  }

  describe(anInstanceOf[WriteConfig]) {
    describe(SHOULD) {
      describe("retrieve") {
        it("the batch size") {

          val batchSize = 25
          emptyConfig.batchSize shouldBe WriteConfig.DEFAULT_BATCH_SIZE_VALUE
          WriteConfig(
            Map(
              WriteConfig.BATCH_SIZE_CONFIG -> s"$batchSize"
            )
          ).batchSize shouldBe batchSize
        }

        it("the index action type") {

          emptyConfig.maybeUserSpecifiedAction shouldBe empty
          emptyConfig.overallAction shouldBe WriteConfig.DEFAULT_ACTION_TYPE
          val action = IndexActionType.UPLOAD
          val configMaps: Seq[Map[String, String]] = Seq(
            action.name(),
            action.toString,
            action.name().toLowerCase,
            action.toString.toUpperCase,
            action.toString.toLowerCase,
          ).map {
            value => Map(
              WriteConfig.ACTION_CONFIG -> value
            )
          }

          forAll(configMaps) {
            configMap =>

              val wConfig = WriteConfig(configMap)
              wConfig.maybeUserSpecifiedAction shouldBe Some(action)
              wConfig.overallAction shouldBe action
          }
        }

        it("the name of the index action type column") {

          val colName = "actionCol"
          emptyConfig.actionColumn shouldBe empty
          WriteConfig(
            Map(
              WriteConfig.INDEX_ACTION_COLUMN_CONFIG -> colName
            )
          ).actionColumn shouldBe Some(colName)
        }

        describe("search field creation options") {
          it("eventually throwing an exception for missing key fields") {

            a[ConfigException] shouldBe thrownBy {
              emptyConfig.searchFieldCreationOptions
            }
          }

          describe("related to") {
            it("field features") {

              val (facetable, filterable) = (Seq("f1"), Seq("f2"))
              val (hidden, searchable, sortable) = (Seq("f3"), Seq("f4"), Seq("f5"))
              val options = WriteConfig(
                Map(
                  fieldOptionKey(WriteConfig.KEY_FIELD_CONFIG) -> keyField,
                  fieldOptionKey(WriteConfig.DISABLE_FACETING_CONFIG) -> facetable.mkString(","),
                  fieldOptionKey(WriteConfig.DISABLE_FILTERING_CONFIG) -> filterable.mkString(","),
                  fieldOptionKey(WriteConfig.HIDDEN_FIELDS_CONFIG) -> hidden.mkString(","),
                  fieldOptionKey(WriteConfig.DISABLE_SEARCH_CONFIG) -> searchable.mkString(","),
                  fieldOptionKey(WriteConfig.DISABLE_SORTING_CONFIG) -> sortable.mkString(","),
                  WriteConfig.INDEX_ACTION_COLUMN_CONFIG -> indexActionColumn
                )
              ).searchFieldCreationOptions

              options.keyField shouldBe keyField
              assertDefinedAndContaining(options.disabledFromFaceting, facetable)
              assertDefinedAndContaining(options.disabledFromFiltering, filterable)
              assertDefinedAndContaining(options.hiddenFields, hidden)
              assertDefinedAndContaining(options.disabledFromSearch, searchable)
              assertDefinedAndContaining(options.disabledFromSorting, sortable)
              options.indexActionColumn shouldBe Some(indexActionColumn)
            }

            it("field analyzers") {

              val aliases: Map[String, (SearchFieldAnalyzerType, LexicalAnalyzerName, Seq[String])] = Map(
                "first" -> (SearchFieldAnalyzerType.ANALYZER, LexicalAnalyzerName.SIMPLE, Seq("a1", "a2")),
                "second" -> (SearchFieldAnalyzerType.SEARCH_ANALYZER, LexicalAnalyzerName.STOP, Seq("a3", "a4")),
                "third" -> (SearchFieldAnalyzerType.INDEX_ANALYZER, LexicalAnalyzerName.IT_MICROSOFT, Seq("a5", "a6"))
              )

              val rawConfig = Map(
                fieldOptionKey(WriteConfig.KEY_FIELD_CONFIG) -> keyField
              ) ++ rawConfigForAnalyzers(aliases)

              val options = WriteConfig(rawConfig).searchFieldCreationOptions
              options.keyField shouldBe keyField
              options.analyzerConfigs shouldBe defined
              val analyzerConfigs = options.analyzerConfigs.get
              analyzerConfigs should have size aliases.size
              forAll(aliases.toSeq) {
                case (alias, (analyzerType, name, onFields)) =>

                  val maybeAnalyzerConfig = analyzerConfigs.find {
                    _.alias.equalsIgnoreCase(alias)
                  }

                  maybeAnalyzerConfig shouldBe defined
                  val analyzerConfig = maybeAnalyzerConfig.get
                  analyzerConfig.`type` shouldBe analyzerType
                  analyzerConfig.name shouldBe name
                  analyzerConfig.fields should contain theSameElementsAs onFields
              }
            }
          }
        }

        it("the similarity algorithm") {

          // No configuration
          emptyConfig.similarityAlgorithm shouldBe empty

          // Invalid case
          a[ConfigException] shouldBe thrownBy {

            WriteConfig(
              Map(
                WriteConfig.SIMILARITY -> createSimpleODataType("#hello")
              )
            ).similarityAlgorithm
          }

          // Valid case
          val (k1, b) = (0.1, 0.2)
          val maybeAlgorithm = WriteConfig(
            Map(
              WriteConfig.SIMILARITY -> createBM25SimilarityAlgorithm(k1, b)
            )
          ).similarityAlgorithm
          maybeAlgorithm shouldBe defined
          maybeAlgorithm.get shouldBe a [BM25SimilarityAlgorithm]
        }

        it("index tokenizers") {

          // No configuration
          emptyConfig.tokenizers shouldBe empty

          // Invalid case
          a [ConfigException] shouldBe thrownBy {

            WriteConfig(
              Map(
                WriteConfig.TOKENIZERS -> createArray(
                  createSimpleODataType("hello")
                )
              )
            ).tokenizers
          }

          // Valid case
          val maybeTokenizers = WriteConfig(
            Map(
              WriteConfig.TOKENIZERS -> createArray(
                createClassicTokenizer("tokenizerName", 20)
              )
            )
          ).tokenizers

          maybeTokenizers shouldBe defined
          val tokenizers = maybeTokenizers.get
          tokenizers should have size 1
          val head = tokenizers.head
          head shouldBe a [ClassicTokenizer]
        }

        it("the set of actions to apply on a Search index") {

          val actions = WriteConfig(
            Map(
              WriteConfig.SIMILARITY -> createBM25SimilarityAlgorithm(0.1, 0.3),
              WriteConfig.TOKENIZERS -> createArray(
                createClassicTokenizer("classicTok", 10)
              )
            )
          ).searchIndexActions

          actions should have size 2
        }
      }
    }
  }
}
