package com.github.jarol.azure.search.spark.sql.connector

import com.azure.core.util.Context
import com.azure.search.documents.SearchDocument
import com.azure.search.documents.indexes.models.SearchField
import com.azure.search.documents.models.SearchOptions
import com.github.jarol.azure.search.spark.sql.connector.clients.JavaClients
import com.github.jarol.azure.search.spark.sql.connector.config.{IOConfig, ReadConfig}
import com.github.jarol.azure.search.spark.sql.connector.schema.InferSchema

import java.time.LocalDateTime
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class ReadSpec extends SparkSpec {

  it("a") {

    /*
    val df = spark.read.format(SearchTableProvider.SHORT_NAME)
      .option(IOConfig.INDEX_CONFIG, "1721203770598-personnel-list")
      .option(ReadConfig.PARTITIONER_CONFIG, classOf[FacetedPartitioner].getName)
      .option(ReadConfig.PARTITIONER_OPTIONS_PREFIX + ReadConfig.PARTITIONER_OPTIONS_FACET_CONFIG, "country")
      .option(ReadConfig.PARTITIONER_OPTIONS_PREFIX + ReadConfig.PARTITIONER_OPTIONS_FACET_QUERY_PARAMETER_CONFIG, "count:24")
      .load().cache()

    df.printSchema()
    df.show(false)
    println(f"Number of partitions: ${df.rdd.getNumPartitions}")
    println(f"Number of rows: ${df.count()}")

     */

    val schema = InferSchema.inferSchema(
      Map(
        IOConfig.END_POINT_CONFIG -> "https://lovappacsd01.search.windows.net",
        IOConfig.API_KEY_CONFIG -> "5E771F200AA3B8FCF37041CDE2AA0D56",
        IOConfig.INDEX_CONFIG -> "v1-reps-1654682555")
    )

    val fields: Seq[SearchField] = JavaClients.forIndex(
      ReadConfig(
        Map(
          IOConfig.END_POINT_CONFIG -> "https://lovappacsd01.search.windows.net",
          IOConfig.API_KEY_CONFIG -> "5E771F200AA3B8FCF37041CDE2AA0D56",
          IOConfig.INDEX_CONFIG -> "v1-reps-1654682555"))
    ).getIndex("v1-reps-1654682555").getFields.asScala

    val doc = JavaClients.doSearch(
      ReadConfig(
        Map(
          IOConfig.END_POINT_CONFIG -> "https://lovappacsd01.search.windows.net",
          IOConfig.API_KEY_CONFIG -> "5E771F200AA3B8FCF37041CDE2AA0D56",
          IOConfig.INDEX_CONFIG -> "v1-reps-1654682555")
      ),
      new SearchOptions()
    ).iterator().asScala.toSeq.head.getDocument(classOf[SearchDocument])

    println(s"Start time: ${LocalDateTime.now()}")
    val result = JavaClients.forSearch(
      ReadConfig(
        Map(
          IOConfig.END_POINT_CONFIG -> "https://lovappacsd01.search.windows.net",
          IOConfig.API_KEY_CONFIG -> "5E771F200AA3B8FCF37041CDE2AA0D56",
          IOConfig.INDEX_CONFIG -> "v1-reps-1654682555"))
    ).search(
      null,
      new SearchOptions()
        .setIncludeTotalCount(true)
        .setTop(1000)
        //.setFilter("firstName eq 'ANDREA'")
        //.setFacets("unknownField")
        ,
      Context.NONE
    )

    Try {
      result.iterator().asScala.toSeq.zipWithIndex.foreach {
        case (sr, i) => println(s"Processing document # ${i + 1}")
      }
    } match {
      case Failure(exception) => println(s"Failure at ${LocalDateTime.now()}")
      case Success(value) => println(s"Success at ${LocalDateTime.now()}")
    }
  }
}
