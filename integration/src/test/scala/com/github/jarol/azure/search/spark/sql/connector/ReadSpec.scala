package com.github.jarol.azure.search.spark.sql.connector

import com.github.jarol.azure.search.spark.sql.connector.core.IndexDoesNotExistException
import com.github.jarol.azure.search.spark.sql.connector.models.SimpleBean
import com.github.jarol.azure.search.spark.sql.connector.write.WriteConfig

import java.time.LocalDate

class ReadSpec
  extends SearchSparkSpec {

  describe("Search dataSource") {
    describe(SHOULD) {
      describe("throw an exception when") {
        it("target index does not exist") {

          val indexName = "non-existing"
          dropIndexIfExists(indexName)
          indexExists(indexName) shouldBe false
          a [IndexDoesNotExistException] shouldBe thrownBy {
            readIndex(indexName, None, None, None)
          }

          dropIndexIfExists(indexName)
        }
      }

      describe("read documents from a Search index") {
        it("that match a filter") {

          val (indexName, id) = ("simple-beans", "hello")
          val input = Seq(
            SimpleBean(id, Some(LocalDate.now())),
            SimpleBean("world", Some(LocalDate.now().plusDays(1)))
          )

          dropIndexIfExists(indexName)
          indexExists(indexName) shouldBe false

          val writeExtraOptions = Map(
            WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.FILTERABLE_FIELDS -> "id"
          )

          writeToIndex(toDF(input), indexName, "id", Some(writeExtraOptions))

          // Wait some time to ensure result consistency
          Thread.sleep(5000)
          indexExists(indexName) shouldBe true
          val df = readIndex(indexName, Some(s"id eq '$id'"), None, Some(schemaOfCaseClass[SimpleBean]))

          val output = toSeqOf[SimpleBean](df)
          output should have size 1
          output.head.id shouldBe id

          dropIndexIfExists(indexName)
          indexExists(indexName) shouldBe false
        }

        it("selecting a set of subfields") {

          val indexName = "select-beans"
          dropIndexIfExists(indexName)
          indexExists(indexName) shouldBe false

          val input = Seq(
            SimpleBean("hello", Some(LocalDate.now())),
            SimpleBean("world", Some(LocalDate.now().plusDays(1)))
          )

          dropIndexIfExists(indexName)
          indexExists(indexName) shouldBe false
          writeToIndex(toDF(input), indexName, "id", None)

          // Wait some time to ensure result consistency
          Thread.sleep(5000)

          val select = Seq("id", "date")
          val df = readIndex(indexName, None, Some(select), None)
          df.count() shouldBe input.size
          df.columns should contain theSameElementsAs select

          dropIndexIfExists(indexName)
          indexExists(indexName) shouldBe false
        }
      }
    }
  }
}
