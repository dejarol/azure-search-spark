package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec

class PredicatePushDowITSpec
  extends BasicSpec {

  it("a") {

    /*
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
      .load().filter(!col("id").isin("first", "second"))

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

     */
  }
}
