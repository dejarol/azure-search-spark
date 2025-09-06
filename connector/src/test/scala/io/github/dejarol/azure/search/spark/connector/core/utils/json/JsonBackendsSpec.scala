package io.github.dejarol.azure.search.spark.connector.core.utils.json

import com.azure.search.documents.indexes.models.{ClassicSimilarityAlgorithm, SimilarityAlgorithm}
import io.github.dejarol.azure.search.spark.connector.core.TestConstants
import io.github.dejarol.azure.search.spark.connector.{BasicSpec, SearchAPIModelFactory}

class JsonBackendsSpec
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

  describe(`object`[JsonBackends.type ]) {
    describe(SHOULD) {
      describe("allow the creation of several deserialization backends, like") {
        it("JSON4s") {

          // TODO
        }

        it("Azure Search REST API models") {

          // Create a backend for deserializing Azure Search REST API models
          val backend = JsonBackends.forAzureModel[SimilarityAlgorithm](SimilarityAlgorithm.fromJson)

          // Test it against a valid case
          val validJson = createSimpleODataType(TestConstants.CLASSIC_SIMILARITY_ALGORITHM)
          val algorithm = backend.deserialize(validJson)
          algorithm shouldBe a [ClassicSimilarityAlgorithm]

          backend.safelyDeserialize(validJson) shouldBe 'right

          // Test it against an invalid case
          val invalidJson = createSimpleODataType("hello")
          a [Throwable] shouldBe thrownBy {
            backend.deserialize(invalidJson)
          }

          backend.safelyDeserialize(invalidJson) shouldBe 'left
        }

        it("array of Azure Search REST API models") {

          val backend = JsonBackends.forAzureArrayOf[SimilarityAlgorithm](SimilarityAlgorithm.fromJson)

          // Valid case
          val validJson = createJsonForArrayOfSimilarityAlgorithms(TestConstants.CLASSIC_SIMILARITY_ALGORITHM, TestConstants.CLASSIC_SIMILARITY_ALGORITHM)
          val array = backend.deserialize(validJson)
          array should have size 2
          val supposedArray = backend.safelyDeserialize(validJson)
          supposedArray shouldBe 'right
          supposedArray.right.get should have size 2

          // Invalid case
          val invalidJson = createJsonForArrayOfSimilarityAlgorithms(TestConstants.CLASSIC_SIMILARITY_ALGORITHM, "hello")
          a [Throwable] shouldBe thrownBy {
            backend.deserialize(invalidJson)
          }

          backend.safelyDeserialize(invalidJson) shouldBe 'left
        }
      }
    }
  }
}
