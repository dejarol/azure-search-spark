package com.github.jarol.azure.search.spark.sql.connector

import com.github.jarol.azure.search.spark.sql.connector.models.SimpleBean
import com.github.jarol.azure.search.spark.sql.connector.read.ReadConfig
import com.github.jarol.azure.search.spark.sql.connector.write.WriteConfig
import org.apache.spark.sql.SaveMode

import java.time.LocalDate

class ReadSpec
  extends SearchSparkSpec {

  /*
  private def readIndex(
                         name: String,
                         filter: Option[String],
                         select: Option[Seq[String]],
                         schema: Option[StructType]
                       ): DataFrame = {

    val extraOptions: Map[String, String] =
    spark.read.format(SearchTableProvider.SHORT_NAME)
      .options(optionsForAuthAndIndex(name))
      .option(ReadConfig.FILTER_CONFIG, s"id eq '$id'")
      .schema(schemaOfCaseClass[SimpleBean])
      .load()
  }

   */

  describe("Search dataSource") {
    describe(SHOULD) {
      describe("throw an exception when") {
        it("target index does not exist") {

        }

        it("there's a schema incompatibility") {

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
          toDF(input).write.format(SearchTableProvider.SHORT_NAME)
            .options(optionsForAuthAndIndex(indexName))
            .option(WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.KEY_FIELD, "id")
            .option(WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.FILTERABLE_FIELDS, "id")
            .mode(SaveMode.Append)
            .save()

          // Wait some time to ensure result consistency
          Thread.sleep(5000)
          indexExists(indexName) shouldBe true
          val df = spark.read.format(SearchTableProvider.SHORT_NAME)
            .options(optionsForAuthAndIndex(indexName))
            .option(ReadConfig.FILTER_CONFIG, s"id eq '$id'")
            .schema(schemaOfCaseClass[SimpleBean])
            .load()

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
          toDF(input).write.format(SearchTableProvider.SHORT_NAME)
            .options(optionsForAuthAndIndex(indexName))
            .option(WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.KEY_FIELD, "id")
            .option(WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.FILTERABLE_FIELDS, "id")
            .mode(SaveMode.Append)
            .save()

          // Wait some time to ensure result consistency
          Thread.sleep(5000)

          val select = Seq("id", "date")
          val df = spark.read.format(SearchTableProvider.SHORT_NAME)
            .options(optionsForAuthAndIndex(indexName))
            .option(ReadConfig.SELECT_CONFIG, select.mkString(","))
            .load()

          df.count() shouldBe input.size
          df.columns should contain theSameElementsAs select

          dropIndexIfExists(indexName)
          indexExists(indexName) shouldBe false
        }
      }
    }
  }
}
