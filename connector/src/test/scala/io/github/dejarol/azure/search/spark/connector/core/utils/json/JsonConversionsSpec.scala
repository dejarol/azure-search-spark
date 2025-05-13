package io.github.dejarol.azure.search.spark.connector.core.utils.json

import com.azure.search.documents.indexes.models.LexicalAnalyzerName
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import io.github.dejarol.azure.search.spark.connector.{BasicSpec, JsonMixIns}

class JsonConversionsSpec
  extends BasicSpec
    with JsonMixIns {

  private lazy val nodeFactory = JsonNodeFactory.instance

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
      }
    }
  }
}
