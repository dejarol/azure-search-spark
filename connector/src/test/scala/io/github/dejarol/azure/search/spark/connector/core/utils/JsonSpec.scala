package io.github.dejarol.azure.search.spark.connector.core.utils

import com.azure.search.documents.indexes.models.{ClassicSimilarityAlgorithm, SimilarityAlgorithm}
import io.github.dejarol.azure.search.spark.connector.core.TestConstants
import io.github.dejarol.azure.search.spark.connector.{BasicSpec, SearchAPIModelFactory}

class JsonSpec
  extends BasicSpec
    with SearchAPIModelFactory {

  /**
   * Create a JSON array of models representing [[SimilarityAlgorithm]](s)
   * @param names algorithm names
   * @return a JSON array of models representing [[SimilarityAlgorithm]](s)
   */

  private def createJsonForArrayOfSimilarityAlgorithms(names: String*): String = {

    names
      .map(createSimpleODataType)
      .mkString("[", ",", "]")
  }

  describe(`object`[Json.type]) {
    describe(SHOULD) {
      describe("deserialize a JSON string to") {
        it("an API model") {

          val unsafeDeserialization = Json.unsafelyReadAzModel[SimilarityAlgorithm](_, SimilarityAlgorithm.fromJson)
          val safeDeserialization = Json.safelyReadAzModel[SimilarityAlgorithm](_, SimilarityAlgorithm.fromJson)

          // Valid case
          val validJson = createSimpleODataType(TestConstants.CLASSIC_SIMILARITY_ALGORITHM)
          val algorithm = unsafeDeserialization(validJson)
          algorithm shouldBe a [ClassicSimilarityAlgorithm]
          safeDeserialization(validJson) shouldBe 'right

          // Invalid case
          val invalidJson = createSimpleODataType("hello")
          a [Throwable] shouldBe thrownBy {
            unsafeDeserialization(invalidJson)
          }

          safeDeserialization(invalidJson) shouldBe 'left
        }

        it("an array of API models") {

          val unsafeDeserialization = Json.unsafelyReadAzModelArray[SimilarityAlgorithm](_, SimilarityAlgorithm.fromJson)
          val safeDeserialization = Json.safelyReadAzModelArray[SimilarityAlgorithm](_, SimilarityAlgorithm.fromJson)

          // Valid case
          val validJson = createJsonForArrayOfSimilarityAlgorithms(TestConstants.CLASSIC_SIMILARITY_ALGORITHM, TestConstants.CLASSIC_SIMILARITY_ALGORITHM)
          val array = unsafeDeserialization(validJson)
          array should have size 2
          val supposedArray = safeDeserialization(validJson)
          supposedArray shouldBe 'right
          supposedArray.right.get should have size 2

          // Invalid case
          val invalidJson = createJsonForArrayOfSimilarityAlgorithms(TestConstants.CLASSIC_SIMILARITY_ALGORITHM, "hello")
          a [Throwable] shouldBe thrownBy {
            unsafeDeserialization(invalidJson)
          }

          safeDeserialization(invalidJson) shouldBe 'left
        }
      }
    }
  }
}
