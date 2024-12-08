package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.{BM25SimilarityAlgorithm, ClassicTokenizer, LexicalAnalyzer, LexicalAnalyzerName, LexicalTokenizer, SearchSuggester, SimilarityAlgorithm, StopAnalyzer}
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

  /**
   * Assert that a Search index option behaves as expected, which means
   *  - when not defined by the user, it should be empty
   *  - when defined but invalid a [[ConfigException]] should be thrown
   *  - when defined and valid, it should be defined
   * @param key configuration key related to the index option
   * @param invalidValue invalid configuration value
   * @param validValue valid configuration value
   * @param getter getter for retrieving the option
   * @param assertion assertion for the result
   * @tparam T option type
   */

  private def assertSearchIndexOption[T](
                                              key: String,
                                              invalidValue: String,
                                              validValue: String,
                                              getter: WriteConfig => Option[T]
                                            )(assertion: T => Unit): Unit = {

    // Given an empty configuration, the result should be empty
    getter(emptyConfig) shouldBe empty

    // Given an invalid configuration, a ConfigException should be thrown
    a[ConfigException] shouldBe thrownBy {
      getter(
        WriteConfig(
          Map(key -> invalidValue)
        )
      )
    }

    // Given a valid configuration, the result should be defined
    val maybeResult = getter(
      WriteConfig(
        Map(
          key -> validValue
        )
      )
    )

    maybeResult shouldBe defined
    assertion(maybeResult.get)
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

        describe("search index extra options, like") {

          it("the similarity algorithm") {

            val (k1, b) = (0.1, 0.3)
            assertSearchIndexOption[SimilarityAlgorithm](
              WriteConfig.SIMILARITY_CONFIG,
              createSimpleODataType("#hello"),
              createBM25SimilarityAlgorithm(k1, b),
              _.similarityAlgorithm
            ) {
              algo =>
                algo shouldBe a [BM25SimilarityAlgorithm]
                val bm25 = algo.asInstanceOf[BM25SimilarityAlgorithm]
                bm25.getK1 shouldBe k1
                bm25.getB shouldBe b
            }
          }

          it("index tokenizers") {

            assertSearchIndexOption[Seq[LexicalTokenizer]](
              WriteConfig.TOKENIZERS_CONFIG,
              createArray(
                createSimpleODataType("hello")
              ),
              createArray(
                createClassicTokenizer("tokenizerName", 20)
              ),
              _.tokenizers
            ) {
              tokenizers =>
                tokenizers should have size 1
                val head = tokenizers.head
                head shouldBe a[ClassicTokenizer]
            }
          }

          it("search suggesters") {

            val (name, fields) = ("countryAndFunction", Seq("country", "function"))
            assertSearchIndexOption[Seq[SearchSuggester]](
              WriteConfig.SEARCH_SUGGESTERS_CONFIG,
              createArray(
                createSimpleODataType("world")
              ),
              createArray(
                createSearchSuggester(name, fields)
              ),
              _.searchSuggesters
            ) {
              suggesters =>
                suggesters should have size 1
                val head = suggesters.head
                head.getName shouldBe name
                head.getSourceFields should contain theSameElementsAs fields
            }
          }

          it("analyzers") {

            val (name, stopWords) = ("stopper", Seq("a", "an", "the"))
            assertSearchIndexOption[Seq[LexicalAnalyzer]](
              WriteConfig.ANALYZERS_CONFIG,
              createArray(
                createSimpleODataType("world")
              ),
              createArray(
                createStopAnalyzer(name, stopWords)
              ),
              _.analyzers
            ) {
              analyzers =>
                analyzers should have size 1
                val head = analyzers.head
                head shouldBe a [StopAnalyzer]
                val stopAnalyzer = head.asInstanceOf[StopAnalyzer]
                stopAnalyzer.getName shouldBe name
                stopAnalyzer.getStopwords should contain theSameElementsAs stopWords
            }
          }

          it("the set of actions to apply on a Search index") {

            val actions = WriteConfig(
              Map(
                WriteConfig.SIMILARITY_CONFIG -> createBM25SimilarityAlgorithm(0.1, 0.3),
                WriteConfig.TOKENIZERS_CONFIG -> createArray(
                  createClassicTokenizer("classicTok", 10)
                ),
                WriteConfig.SEARCH_SUGGESTERS_CONFIG -> createArray(
                  createSearchSuggester("descriptionSuggester", Seq("description"))
                ),
                WriteConfig.ANALYZERS_CONFIG -> createArray(
                  createStopAnalyzer("stop", Seq("a", "the"))
                )
              )
            ).searchIndexActions

            actions should have size 4
          }
        }
      }
    }
  }
}
