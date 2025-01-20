package io.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.{ClassicSimilarityAlgorithm, SearchIndex}
import io.github.jarol.azure.search.spark.sql.connector.core.BasicSpec

import scala.language.implicitConversions

class SearchIndexOperationsSpec
  extends BasicSpec {

  /**
   * Implicit conversion from a Search index to its 'operations' counterpart
   * @param index a Search index definition
   * @return an instance of [[SearchIndexOperations]]
   */

  private implicit def toIndexOperations(index: SearchIndex): SearchIndexOperations = new SearchIndexOperations(index)

  describe(anInstanceOf[SearchIndexOperations]) {
    describe(SHOULD) {
      it("apply some actions") {

        val index = new SearchIndex("name")
        index.getSimilarity shouldBe null
        val transformedIndex = index.applyActions(
          SearchIndexActions.forSettingSimilarityAlgorithm(
            new ClassicSimilarityAlgorithm()
          )
        )

        transformedIndex.getSimilarity shouldBe a [ClassicSimilarityAlgorithm]
      }
    }
  }
}
