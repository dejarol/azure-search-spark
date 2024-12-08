package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models._
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, JavaScalaConverters}

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

  describe(`object`[SearchIndexActions.type ]) {
    describe(SHOULD) {
      describe("provide methods for creating actions that") {

        it("set a similarity algorithm") {

          val algorithm = new ClassicSimilarityAlgorithm
          emptyIndex.getSimilarity shouldBe null
          val transformedIndex = SearchIndexActions.forSettingSimilarityAlgorithm(algorithm).apply(emptyIndex)
          transformedIndex.getSimilarity shouldBe algorithm
        }

        it("set tokenizers") {

          val tokenizers = Seq(
            new ClassicTokenizer("classic"),
            new EdgeNGramTokenizer("edgeNGram")
          )

          emptyIndex.getTokenizers shouldBe null
          val transformedIndex = SearchIndexActions.forSettingTokenizers(tokenizers).apply(emptyIndex)
          transformedIndex.getTokenizers should contain theSameElementsAs tokenizers
        }

        it("set suggesters") {

          val suggesters = Seq(
            createSuggester("first", Seq("hello", "world")),
            createSuggester("second", Seq("john", "doe"))
          )

          emptyIndex.getSuggesters shouldBe null
          val transformedIndex = SearchIndexActions.forSettingSuggesters(suggesters).apply(emptyIndex)
          transformedIndex.getSuggesters should contain theSameElementsAs suggesters
        }

        it("set analyzers") {

          val analyzers = Seq(
            new StopAnalyzer("stop")
          )
          emptyIndex.getAnalyzers shouldBe null
          val transformedIndex = SearchIndexActions.forSettingAnalyzers(analyzers).apply(emptyIndex)
          transformedIndex.getAnalyzers should contain theSameElementsAs analyzers
        }
      }
    }
  }
}
