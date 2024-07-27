package com.github.jarol.azure.search.spark.sql.connector

import com.github.jarol.azure.search.spark.sql.connector.config.{ReadConfig, SearchConfig}

import scala.collection.JavaConverters._

class ReadSpec extends SparkSpec {

  it("a") {

    val df = spark.read.format(SearchTableProvider.SHORT_NAME)
      .option(SearchConfig.INDEX_CONFIG, "1721203770598-personnel-list")
      .option(ReadConfig.FILTER_CONFIG, "firstName eq 'FRANCESCO'")
      .load().cache()

    df.printSchema()
    df.show(false)
    println(df.count())

    /*
    val result = ClientFactory.searchClient(
      ReadConfig(
        Map(
          SearchConfig.END_POINT_CONFIG -> "https://lohrwkpeacss01.search.windows.net",
          SearchConfig.API_KEY_CONFIG -> "3F491488E774609119C10968C6D47634",
          SearchConfig.INDEX_CONFIG -> "1721203770598-personnel-list"))
    ).search(
      null,
      new SearchOptions()
        .setIncludeTotalCount(true)
        .setFacets("minHcLevel,count:20"),
      Context.NONE
    )

    val totCount = result.getTotalCount
    val facets = result.getFacets
    val x = facets.asScala.flatMap {
      case (str, results) => results.asScala.map(_.getCount)
    }.toSeq.reduce( _ + _)

     */

    val a = 1
  }
}
