package io.github.dejarol.azure.search.spark.connector.core.config

import io.github.dejarol.azure.search.spark.connector.SearchITSpec
import io.github.dejarol.azure.search.spark.connector.models.{PushdownBean, SimpleBean}
import org.apache.spark.sql.Encoders

class SearchIOConfigITSpec
  extends SearchITSpec {

  private lazy val simpleBeansIndex = "simple-beans"
  private lazy val pushdownBeansIndex = "pushdown-beans"
  private lazy val config = new SearchIOConfig(
    optionsForAuthAndIndex(simpleBeansIndex)
  )

  describe(anInstanceOf[SearchIOConfig]) {
    describe(SHOULD) {
      it("evaluate the existence of an index") {

        // Before index creation, all existence assertions should fail
        indexExists(simpleBeansIndex) shouldBe false
        config.indexExists(simpleBeansIndex) shouldBe false
        config.indexExists shouldBe false

        createIndexFromSchemaOf[SimpleBean](simpleBeansIndex)

        indexExists(simpleBeansIndex) shouldBe true
        config.indexExists(simpleBeansIndex) shouldBe true
        config.indexExists shouldBe true

        // Clean up
        dropIndexIfExists(simpleBeansIndex, sleep = true)
      }

      it("list all existing indexes") {

        // Before index creation, the list of existing indexes should be empty
        listIndexes() shouldBe empty
        config.listIndexes shouldBe empty

        createIndexFromSchemaOf[SimpleBean](simpleBeansIndex)
        createIndexFromSchemaOf[PushdownBean](pushdownBeansIndex)

        config.listIndexes.map(_.getName) should contain theSameElementsAs Seq(simpleBeansIndex, pushdownBeansIndex)

        // Clean up
        dropIndexIfExists(simpleBeansIndex, sleep = false)
        dropIndexIfExists(pushdownBeansIndex, sleep = true)
      }

      it("get the fields of an existing index") {

        createIndexFromSchemaOf[SimpleBean](simpleBeansIndex)

        val indexFields = config.getSearchIndexFields
        val schemaOfSimpleBean = Encoders.product[SimpleBean].schema
        indexFields should have size schemaOfSimpleBean.size
        indexFields.map(_.getName) should contain theSameElementsAs schemaOfSimpleBean.fieldNames

        // Clean up
        dropIndexIfExists(simpleBeansIndex, sleep = true)
      }
    }
  }
}
