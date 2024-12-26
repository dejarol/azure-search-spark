package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models._
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, JavaScalaConverters}

import java.lang.{Double => JDouble}
import java.util.{List => JList}

class SearchIndexActionsSpec
  extends BasicSpec {

  private lazy val emptyIndex = new SearchIndex("hello")

  /**
   * Create an instance of [[SearchSuggester]]
   * @param name name
   * @param fields suggester fields
   * @return an instance of [[SearchSuggester]]
   */

  private def createSuggester(
                               name: String,
                               fields: Seq[String]
                             ): SearchSuggester = {
    new SearchSuggester(
      name,
      JavaScalaConverters.seqToList(fields)
    )
  }

  /**
   * Create a [[CharFilter]] instance
   * @param name name
   * @param mappings mappings
   * @return a [[CharFilter]] instance
   */

  private def createMappingCharFilter(
                                       name: String,
                                       mappings: Seq[String]
                                     ): CharFilter = {

    new MappingCharFilter(
      name,
      JavaScalaConverters.seqToList(mappings)
    )
  }

  /**
   * Assert the effect of a [[SearchIndexAction]]
   * @param getter function for getting the result of the action
   * @param action action to test
   * @param assertion assertion to run on action result
   * @tparam T action result type
   */

  private def assertEffectOfAction[T](
                                       getter: SearchIndex => T,
                                       action: SearchIndexAction
                                     )(
                                       assertion: T => Unit
                                     ): Unit = {

    getter(emptyIndex) shouldBe null.asInstanceOf[T]
    val actionEffect: T = getter(action.apply(emptyIndex))
    assertion(actionEffect)
  }

  describe(`object`[SearchIndexActions.type ]) {
    describe(SHOULD) {
      describe("provide methods for creating actions that") {

        it("set a similarity algorithm") {

          val algorithm = new ClassicSimilarityAlgorithm
          assertEffectOfAction[SimilarityAlgorithm](
            _.getSimilarity,
            SearchIndexActions.forSettingSimilarityAlgorithm(algorithm)
          ) {
            _ shouldBe algorithm
          }
        }

        it("set tokenizers") {

          val tokenizers = Seq(
            new ClassicTokenizer("classic"),
            new EdgeNGramTokenizer("edgeNGram")
          )

          assertEffectOfAction[JList[LexicalTokenizer]](
            _.getTokenizers,
            SearchIndexActions.forSettingTokenizers(tokenizers)
          ) {
            _ should contain theSameElementsAs tokenizers
          }
        }

        it("set suggesters") {

          val suggesters = Seq(
            createSuggester("first", Seq("hello", "world")),
            createSuggester("second", Seq("john", "doe"))
          )

          assertEffectOfAction[JList[SearchSuggester]](
            _.getSuggesters,
            SearchIndexActions.forSettingSuggesters(suggesters)
          ) {
            _ should contain theSameElementsAs suggesters
          }
        }

        it("set analyzers") {

          val analyzers: Seq[LexicalAnalyzer] = Seq(
            new StopAnalyzer("stop")
          )

          assertEffectOfAction[JList[LexicalAnalyzer]](
            _.getAnalyzers,
            SearchIndexActions.forSettingAnalyzers(analyzers)
          ) {
            _ should contain theSameElementsAs analyzers
          }
        }

        it("set char filters") {

          val charFilters = Seq(
            createMappingCharFilter("first", Seq("a=>b")),
            createMappingCharFilter("second", Seq("c=>d"))
          )

          assertEffectOfAction[JList[CharFilter]](
            _.getCharFilters,
            SearchIndexActions.forSettingCharFilters(charFilters)
          ) {
            _ should contain theSameElementsAs charFilters
          }
        }

        it("set scoring profiles") {

          val (name, weights) = (
            "profileName",
            Map(
              "hotel" -> JDouble.valueOf(0.2),
              "description" -> JDouble.valueOf(0.5)
            )
          )

          val scoringProfiles = Seq(
            new ScoringProfile(name)
              .setTextWeights(
                new TextWeights(
                  JavaScalaConverters.scalaMapToJava(weights)
                )
              )
          )

          assertEffectOfAction[JList[ScoringProfile]](
            _.getScoringProfiles,
            SearchIndexActions.forSettingScoringProfiles(scoringProfiles)
          ) {
            profiles =>
              profiles should have size 1
              val head = profiles.get(0)
              head.getName shouldBe name
              val actualWeights = head.getTextWeights.getWeights
              forAll(weights.keySet) {
                k =>
                  actualWeights should contain key k
                  actualWeights.get(k) shouldBe weights(k)
              }
          }
        }

        it("set token filters") {

          val tokenFilters = Seq(
            new PatternReplaceTokenFilter("name", "the", "")
          )

          assertEffectOfAction[JList[TokenFilter]](
            _.getTokenFilters,
            SearchIndexActions.forSettingTokenFilters(tokenFilters)
          ) {
            _ should contain theSameElementsAs tokenFilters
          }
        }
      }
    }
  }
}
