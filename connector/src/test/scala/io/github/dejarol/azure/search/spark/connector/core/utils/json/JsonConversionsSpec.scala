package io.github.dejarol.azure.search.spark.connector.core.utils.json

import com.azure.search.documents.indexes.models.LexicalAnalyzerName
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import io.github.dejarol.azure.search.spark.connector.{BasicSpec, JsonMixIns}

class JsonConversionsSpec
  extends BasicSpec
    with JsonMixIns {

  private lazy val nodeFactory = JsonNodeFactory.instance

  /**
   * Creates an array node with the provided values
   * @param values values to append to the node
   * @param nodeFactoryFunction function for creating inner array nodes
   * @tparam T value type
   * @return an array node holding a JSON node for each value
   */

  private def createArrayNodeOf[T](values: Seq[T])(nodeFactoryFunction: T => JsonNode): JsonNode = {

    values.foldLeft(
      nodeFactory.arrayNode()
    ) {
      case (node, v) => node.add(
        nodeFactoryFunction(v)
      )
    }
  }

  describe(`object`[JsonConversions.type ]) {
    describe(SHOULD) {
      describe("provide conversions for") {
        it("strings") {

          val (strValue, stringConversion) = ("hello", JsonConversions.StringConversion)
          val stringNode = nodeFactory.textNode(strValue)
          stringConversion.isDefinedAt(stringNode) shouldBe true
          stringConversion(stringNode) shouldBe strValue

          stringConversion.isDefinedAt(nodeFactory.numberNode(44)) shouldBe false
        }

        it("booleans") {

          val (boolValue, booleanConversion) = (true, JsonConversions.BooleanConversion)
          val boolNode = nodeFactory.booleanNode(boolValue)
          booleanConversion.isDefinedAt(boolNode) shouldBe true
          booleanConversion(boolNode) shouldBe boolValue

          booleanConversion.isDefinedAt(nodeFactory.numberNode(44)) shouldBe false
        }

        it("integers") {

          val (intValue, integerConversion) = (44, JsonConversions.IntConversion)
          val intNode = nodeFactory.numberNode(intValue)
          integerConversion.isDefinedAt(intNode) shouldBe true
          integerConversion(intNode) shouldBe intValue

          integerConversion.isDefinedAt(nodeFactory.textNode("hello")) shouldBe false
        }

        it("lexical analyzer names") {

          val (analyzerValue, nameConversion) = (LexicalAnalyzerName.BG_MICROSOFT, JsonConversions.LexicalAnalyzerNameConversion)
          val validNode = nodeFactory.textNode(analyzerValue.toString)
          nameConversion.isDefinedAt(validNode) shouldBe true
          nameConversion(validNode) shouldBe analyzerValue

          // Assert that definition is false for other nodes, as well as for text nodes with invalid values
          nameConversion.isDefinedAt(nodeFactory.numberNode(44)) shouldBe false
          val invalidNode = nodeFactory.textNode("invalid")
          nameConversion.isDefinedAt(invalidNode) shouldBe false
        }

        describe("collections of") {
          it("strings") {

            val innerValues = Seq("hello", "world")
            val arrayOfStrings = createArrayNodeOf[String](innerValues)(nodeFactory.textNode)
            val arrayConversion = JsonConversions.forArrayOf(JsonConversions.StringConversion)
            arrayConversion.isDefinedAt(arrayOfStrings) shouldBe true
            arrayConversion(arrayOfStrings) should contain theSameElementsAs Seq("hello", "world")

            // Should not be defined for a
            // [1] non-array node
            // [2] array node with numbers
            arrayConversion.isDefinedAt(nodeFactory.numberNode(44)) shouldBe false
            val arrayOfNumbers = createArrayNodeOf[Int](Seq(1, 2, 3))(nodeFactory.numberNode)
            arrayConversion.isDefinedAt(arrayOfNumbers) shouldBe false
          }
        }
      }
    }
  }
}
