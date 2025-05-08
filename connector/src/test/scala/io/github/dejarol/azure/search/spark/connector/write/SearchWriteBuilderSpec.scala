package io.github.dejarol.azure.search.spark.connector.write

import com.azure.search.documents.indexes.models.{ClassicSimilarityAlgorithm, SearchIndex}
import io.github.dejarol.azure.search.spark.connector.BasicSpec

class SearchWriteBuilderSpec
  extends BasicSpec {

  describe(`object`[SearchWriteBuilder.type ]) {
    describe(SHOULD) {
      it("apply some actions on a Search index") {

        val index = new SearchIndex("name")
        index.getSimilarity shouldBe null

        val actions = Seq(
          SearchIndexActions.forSettingSimilarityAlgorithm(
            new ClassicSimilarityAlgorithm()
          )
        )

        val transformedIndex = SearchWriteBuilder.applyActionsToSearchIndex(index, actions)
        transformedIndex.getSimilarity shouldBe a [ClassicSimilarityAlgorithm]
      }
    }
  }
}
