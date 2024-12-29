package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.SearchSparkITSpec
import com.github.jarol.azure.search.spark.sql.connector.core.Constants
import com.github.jarol.azure.search.spark.sql.connector.models.SimpleBean
import org.apache.spark.sql.functions.col

import java.time.LocalDate

class PredicatePushDowITSpec
  extends SearchSparkITSpec{

  it("a") {

    val indexName = "predicate-pushdown"
    dropIndexIfExists(indexName, sleep = false)
    createIndexFromSchemaOf[SimpleBean](indexName)
    writeDocuments[SimpleBean](
      indexName,
      Seq(
        SimpleBean("first", None),
        SimpleBean("second", Some(LocalDate.now()))
      )
    )

    val df = spark.read.format(Constants.DATASOURCE_NAME)
      .options(optionsForAuthAndIndex(indexName))
      .load().filter(col("id").startsWith("f"))

    df.explain()
    dropIndexIfExists(indexName, sleep = false)
  }
}
