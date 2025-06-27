package io.github.dejarol.azure.search.spark.connector.write.config

import io.github.dejarol.azure.search.spark.connector.SearchITSpec
import io.github.dejarol.azure.search.spark.connector.models.SimpleBean

class WriteConfigITSpec
  extends SearchITSpec {

  private lazy val writeConfig = WriteConfig(optionsForAuth)

  describe(anInstanceOf[WriteConfig]) {
    describe(SHOULD) {
      it("drop an existing index") {

        val indexName = "simple-beans"
        createIndexFromSchemaOf[SimpleBean](indexName)
        indexExists(indexName) shouldBe true

        writeConfig.deleteIndex(indexName)
        indexExists(indexName) shouldBe false
      }

      describe("create an index from a name and a schema") {
        it("todo") {

          // TODO: implement here tests related to index creation (from SearchWriteBuilderITSpec)
        }
      }
    }
  }
}
