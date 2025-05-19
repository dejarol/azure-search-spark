package io.github.dejarol.azure.search.spark.connector.core.utils.json

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import io.github.dejarol.azure.search.spark.connector.{BasicSpec, JsonMixIns}

class JsonNodeOperationsSpec
  extends BasicSpec
    with JsonMixIns {

  import JsonNodeOperations._
  import JsonConversions._

  private lazy val nodeFactory = JsonNodeFactory.instance
  private lazy val (keyLabel, stringValue) = ("key", "hello")
  private lazy val nodeWithStringValue = nodeFactory.objectNode().put(keyLabel, stringValue)

  describe(anInstanceOf[JsonNode]) {
    describe(SHOULD) {
      describe("retrieve a typed value") {
        it("safely") {

          val maybeStringValue = nodeWithStringValue.safelyGetAs[String](keyLabel)(StringConversion)
          maybeStringValue shouldBe defined
          maybeStringValue.get shouldBe stringValue

          // Assert that the retrieval fails for an unexisting key
          nodeWithStringValue.safelyGetAs[String](stringValue)(StringConversion) shouldBe empty

          // Assert that the retrieval fails for an existing key with different type
          nodeWithStringValue.safelyGetAs[Boolean]("key")(BooleanConversion) shouldBe empty
        }

        it("unsafely") {

          nodeWithStringValue.getAs[String](keyLabel)(StringConversion) shouldBe stringValue
          an[IllegalArgumentException] shouldBe thrownBy {
            nodeWithStringValue.getAs[String](stringValue)(StringConversion)
          }

          an[IllegalArgumentException] shouldBe thrownBy {
            nodeWithStringValue.getAs[Boolean](keyLabel)(BooleanConversion)
          }
        }
      }
    }
  }
}
