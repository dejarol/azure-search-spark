package com.github.jarol.azure.search.spark.sql.connector

import com.github.jarol.azure.search.spark.sql.connector.core.Constants
import com.github.jarol.azure.search.spark.sql.connector.models.SimpleBean
import com.github.jarol.azure.search.spark.sql.connector.read.ReadConfig
import com.github.jarol.azure.search.spark.sql.connector.write.WriteConfig
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.time.LocalDate

class ReadSpec
  extends SearchSparkSpec {

  private def readIndex(
                         name: String,
                         filter: Option[String],
                         select: Option[Seq[String]],
                         schema: Option[StructType]
                       ): DataFrame = {

    val extraOptions = Map(
      ReadConfig.FILTER_CONFIG -> filter,
      ReadConfig.SELECT_CONFIG -> select.map(_.mkString(","))
    ).collect {
      case (k, Some(v)) => (k, v)
    }

    val reader = spark.read.format(Constants.DATASOURCE_NAME)
      .options(optionsForAuthAndIndex(name))
      .options(extraOptions)

    schema match {
      case Some(value) => reader.schema(value).load(name)
      case None => reader.load(name)
    }
  }

  ignore("Search dataSource") {
    describe(SHOULD) {
      describe("throw an exception when") {
        it("target index does not exist") {

          val indexName = "non-existing"
          dropIndexIfExists(indexName)
          indexExists(indexName) shouldBe false
          readIndex(
            indexName,
            None,
            None,
            None
          )

          dropIndexIfExists(indexName)
        }
      }

      ignore("read documents from a Search index") {
        it("that match a filter") {

          val (indexName, id) = ("simple-beans", "hello")
          val input = Seq(
            SimpleBean(id, Some(LocalDate.now())),
            SimpleBean("world", Some(LocalDate.now().plusDays(1)))
          )

          dropIndexIfExists(indexName)
          indexExists(indexName) shouldBe false
          toDF(input).write.format(Constants.DATASOURCE_NAME)
            .options(optionsForAuthAndIndex(indexName))
            .option(WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.KEY_FIELD, "id")
            .option(WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.FILTERABLE_FIELDS, "id")
            .mode(SaveMode.Append)
            .save()

          // Wait some time to ensure result consistency
          Thread.sleep(5000)
          indexExists(indexName) shouldBe true
          val df = readIndex(
            indexName,
            Some(s"id eq '$id'"),
            None,
            Some(schemaOfCaseClass[SimpleBean])
          )

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
          toDF(input).write.format(Constants.DATASOURCE_NAME)
            .options(optionsForAuthAndIndex(indexName))
            .option(WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.KEY_FIELD, "id")
            .option(WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.FILTERABLE_FIELDS, "id")
            .mode(SaveMode.Append)
            .save()

          // Wait some time to ensure result consistency
          Thread.sleep(5000)

          val select = Seq("id", "date")
          val df = readIndex(
            indexName,
            None,
            Some(select),
            None
          )

          df.count() shouldBe input.size
          df.columns should contain theSameElementsAs select

          dropIndexIfExists(indexName)
          indexExists(indexName) shouldBe false
        }
      }
    }
  }
}
