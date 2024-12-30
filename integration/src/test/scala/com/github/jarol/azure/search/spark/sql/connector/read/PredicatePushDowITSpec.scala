package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.SearchSparkITSpec
import com.github.jarol.azure.search.spark.sql.connector.core.Constants
import com.github.jarol.azure.search.spark.sql.connector.models.PushdownBean
import com.github.jarol.azure.search.spark.sql.connector.read.filter.V2ExpressionODataBuilder
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.functions.col

import java.sql.Timestamp
import java.time.Instant

class PredicatePushDowITSpec
  extends SearchSparkITSpec {

  it("a") {

    val indexName = "predicate-pushdown"
    dropIndexIfExists(indexName, sleep = false)
    createIndexFromSchemaOf[PushdownBean](indexName)
    writeDocuments[PushdownBean](
      indexName,
      Seq(
        PushdownBean("first", None, None),
        PushdownBean("second", Some(1), Some(Timestamp.from(Instant.now())))
      )
    )

    val df = spark.read.format(Constants.DATASOURCE_NAME)
      .options(optionsForAuthAndIndex(indexName))
      .load().filter(col("id").contains("rst"))

    val customScanExec = df.queryExecution.executedPlan.collect {
      case scan: BatchScanExec =>
        scan
    }.headOption.getOrElse(
      fail("BatchScanExec not found in the executed plan")
    )

    customScanExec.scan match {
      case scan: SearchScan =>
        val pushedFilters = scan.pushedPredicates
        println(s"Pushed filters: ${pushedFilters.map(V2ExpressionODataBuilder.build).collect {
          case Some(value) => value
        }.mkString("(", ",", ")")}")
    }

    println("Logical plan:" + df.queryExecution.logical.toString())
    df.explain(true)
    dropIndexIfExists(indexName, sleep = false)
  }
}
