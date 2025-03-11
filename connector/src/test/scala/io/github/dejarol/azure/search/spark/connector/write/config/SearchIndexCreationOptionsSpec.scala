package io.github.dejarol.azure.search.spark.connector.write.config

import com.azure.search.documents.indexes.models._
import io.github.dejarol.azure.search.spark.connector.{BasicSpec, SearchAPIModelFactory}
import io.github.dejarol.azure.search.spark.connector.core.config.SearchConfig
import io.github.dejarol.azure.search.spark.connector.core.BasicSpec

class SearchIndexCreationOptionsSpec
  extends BasicSpec
    with SearchAPIModelFactory {

  private lazy val emptyConfig = createOptions(Map.empty)

  /**
   * Create a set of options from a raw configuration map
   * @param map raw configuration
   * @return an instance of [[SearchIndexCreationOptions]]
   */

  private def createOptions(map: Map[String, String]): SearchIndexCreationOptions = {

    SearchIndexCreationOptions(
      new SearchConfig(map)
    )
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

  private def assertBehaviorForIndexOption[T](
                                               key: String,
                                               invalidValue: String,
                                               validValue: String,
                                               getter: SearchIndexCreationOptions => Option[T]
                                             )(
                                               assertion: T => Unit
                                             ): Unit = {

    // Given an empty configuration, the result should be empty
    getter(emptyConfig) shouldBe empty

    // Given an invalid configuration, a ConfigException should be thrown
    a[ConfigException] shouldBe thrownBy {
      getter(
        createOptions(
          Map(key -> invalidValue)
        )
      )
    }

    // Given a valid configuration, the result should be defined
    val maybeResult = getter(
      createOptions(
        Map(key -> validValue)
      )
    )

    maybeResult shouldBe defined
    assertion(maybeResult.get)
  }

  describe(anInstanceOf[SearchIndexCreationOptions]) {
    describe(SHOULD) {
      describe("search index extra options, like") {

        it("the similarity algorithm") {

          val (k1, b) = (0.1, 0.3)
          assertBehaviorForIndexOption[SimilarityAlgorithm](
            SearchIndexCreationOptions.SIMILARITY_CONFIG,
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

          assertBehaviorForIndexOption[Seq[LexicalTokenizer]](
            SearchIndexCreationOptions.TOKENIZERS_CONFIG,
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
          assertBehaviorForIndexOption[Seq[SearchSuggester]](
            SearchIndexCreationOptions.SUGGESTERS_CONFIG,
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
          assertBehaviorForIndexOption[Seq[LexicalAnalyzer]](
            SearchIndexCreationOptions.ANALYZERS_CONFIG,
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

        it("char filters") {

          val (name, mappings) = ("charFilterName", Seq("first_name", "last_name"))
          assertBehaviorForIndexOption[Seq[CharFilter]](
            SearchIndexCreationOptions.CHAR_FILTERS_CONFIG,
            createArray(
              createSimpleODataType("john"),
            ),
            createArray(
              createMappingCharFilter(name, mappings)
            ),
            _.charFilters
          ) {
            charFilters =>
              charFilters should have size 1
              val head = charFilters.head
              head shouldBe a [MappingCharFilter]
              val mappingCharFilter = head.asInstanceOf[MappingCharFilter]
              mappingCharFilter.getName shouldBe name
              mappingCharFilter.getMappings should contain theSameElementsAs mappings
          }
        }

        it("scoring profiles") {

          val (name, weights) = (
            "profileName",
            Map(
              "hotel" -> 0.2,
              "description" -> 0.5
            )
          )

          assertBehaviorForIndexOption[Seq[ScoringProfile]](
            SearchIndexCreationOptions.SCORING_PROFILES_CONFIG,
            createArray(
              createSimpleODataType("world")
            ),
            createArray(
              createScoringProfile(name, weights)
            ),
            _.scoringProfiles
          ) {
            profiles =>
              profiles should have size 1
              val head = profiles.head
              head.getName shouldBe name
              val actualWeights = head.getTextWeights.getWeights
              forAll(weights.keySet) {
                k =>
                  actualWeights should contain key k
                  actualWeights.get(k) shouldBe weights(k)
              }
          }
        }

        it("token filters") {

          val (name, pattern, replacement) = ("articlePattern", "article", "replace")
          assertBehaviorForIndexOption[Seq[TokenFilter]](
            SearchIndexCreationOptions.TOKEN_FILTERS_CONFIG,
            createArray(
              createSimpleODataType("hello")
            ),
            createArray(
              createPatternReplaceTokenFilter(name, pattern, replacement)
            ),
            _.tokenFilters
          ) {
            filters =>
              filters should have size 1
              val head = filters.head
              head shouldBe a [PatternReplaceTokenFilter]
              val filter = head.asInstanceOf[PatternReplaceTokenFilter]
              filter.getName shouldBe name
              filter.getPattern shouldBe pattern
              filter.getReplacement shouldBe replacement
          }
        }

        it("CORS options") {

          val (allowedOrigins, maxAge) = (Seq("first"), 15)
          assertBehaviorForIndexOption[CorsOptions](
            SearchIndexCreationOptions.CORS_OPTIONS_CONFIG,
            "{}",
            createCorsOptions(allowedOrigins, maxAge),
            _.corsOptions
          ) {
            cors =>
              cors.getAllowedOrigins should contain theSameElementsAs allowedOrigins
              cors.getMaxAgeInSeconds shouldBe maxAge
          }
        }

        it("default scoring profile") {

          val name = "profileName"
          emptyConfig.defaultScoringProfile shouldBe empty
          createOptions(
            Map(
              SearchIndexCreationOptions.DEFAULT_SCORING_PROFILE_CONFIG -> name
            )
          ).defaultScoringProfile shouldBe Some(name)
        }

        it("the set of actions to apply on a Search index") {

          val actions = SearchIndexCreationOptions(
            createOptions(
              Map(
                SearchIndexCreationOptions.SIMILARITY_CONFIG -> createBM25SimilarityAlgorithm(0.1, 0.3),
                SearchIndexCreationOptions.TOKENIZERS_CONFIG -> createArray(
                  createClassicTokenizer("classicTok", 10)
                ),
                SearchIndexCreationOptions.SUGGESTERS_CONFIG -> createArray(
                  createSearchSuggester("descriptionSuggester", Seq("description"))
                ),
                SearchIndexCreationOptions.ANALYZERS_CONFIG -> createArray(
                  createStopAnalyzer("stop", Seq("a", "the"))
                ),
                SearchIndexCreationOptions.SCORING_PROFILES_CONFIG -> createArray(
                  createScoringProfile("profileName", Map("hotel" -> 0.5))
                ),
                SearchIndexCreationOptions.TOKEN_FILTERS_CONFIG -> createArray(
                  createPatternReplaceTokenFilter("filterName", "patt", "repl")
                ),
                SearchIndexCreationOptions.CORS_OPTIONS_CONFIG -> createCorsOptions(
                  Seq("second"), 10
                ),
                SearchIndexCreationOptions.DEFAULT_SCORING_PROFILE_CONFIG -> "profileName"
              )
            )
          ).searchIndexActions

          actions should have size 8
        }
      }
    }
  }
}
