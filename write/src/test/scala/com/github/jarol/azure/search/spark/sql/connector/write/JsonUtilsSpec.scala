package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.{ClassicSimilarityAlgorithm, SimilarityAlgorithm}
import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import org.scalatest.EitherValues

class JsonUtilsSpec
  extends BasicSpec
    with EitherValues {

  private lazy val validAlgorithmName = "#Microsoft.Azure.Search.ClassicSimilarity"

  /**
   * Create a json string representing a [[SimilarityAlgorithm]]
   * @param name algorithm name
   * @return a json string representing a [[SimilarityAlgorithm]]
   */

  private def createJsonForSimilarityAlgorithm(name: String): String = {

    s"""
       |{
       | "@odata.type": "$name"
       |}""".stripMargin
  }

  /**
   * Create a json array of models representing [[SimilarityAlgorithm]](s)
   * @param names algorithm names
   * @return a json array of models representing [[SimilarityAlgorithm]](s)
   */

  private def createJsonForArrayOfSimilarityAlgorithms(names: String*): String = {

    names
      .map(createJsonForSimilarityAlgorithm)
      .mkString("[", ",", "]")
  }

  describe(`object`[JsonUtils.type]) {
    describe(SHOULD) {
      describe("deserialize a json string to") {
        it("an Azure model") {

          val unsafeDeserialization = JsonUtils.unsafelyReadAzModel[SimilarityAlgorithm](_, SimilarityAlgorithm.fromJson)
          val safeDeserialization = JsonUtils.safelyReadAzModel[SimilarityAlgorithm](_, SimilarityAlgorithm.fromJson)

          // Valid case
          val validJson = createJsonForSimilarityAlgorithm(validAlgorithmName)
          val algorithm = unsafeDeserialization(validJson)
          algorithm shouldBe a [ClassicSimilarityAlgorithm]
          safeDeserialization(validJson) shouldBe 'right

          // Invalid case
          val invalidJson = createJsonForSimilarityAlgorithm("hello")
          a [Throwable] shouldBe thrownBy {
            unsafeDeserialization(invalidJson)
          }

          safeDeserialization(invalidJson) shouldBe 'left
        }

        it("an array of Azure models") {

          val unsafeDeserialization = JsonUtils.unsafelyReadAzModelArray[SimilarityAlgorithm](_, SimilarityAlgorithm.fromJson)
          val safeDeserialization = JsonUtils.safelyReadAzModelArray[SimilarityAlgorithm](_, SimilarityAlgorithm.fromJson)

          // Valid case
          val validJson = createJsonForArrayOfSimilarityAlgorithms(validAlgorithmName, validAlgorithmName)
          val array = unsafeDeserialization(validJson)
          array should have size 2
          val supposedArray = safeDeserialization(validJson)
          supposedArray shouldBe 'right
          supposedArray.right.get should have size 2

          // Invalid case
          val invalidJson = createJsonForArrayOfSimilarityAlgorithms(validAlgorithmName, "hello")
          a [Throwable] shouldBe thrownBy {
            unsafeDeserialization(invalidJson)
          }

          safeDeserialization(invalidJson) shouldBe 'left
        }
      }
    }
  }
}
