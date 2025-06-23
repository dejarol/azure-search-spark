package io.github.dejarol.azure.search.spark.connector

import org.apache.spark.sql.SparkSession

class SearchCatalogITSpec
  extends BasicSpec {

  private final lazy val spark = SparkSession.builder()
    .appName(this.getClass.getSimpleName)
    .master("local[*]")
    .config("spark.sql.catalog.search", classOf[SearchCatalog].getName)
    .config("spark.sql.catalog.search.endpoint", "abc")
    .config("spark.sql.catalog.search.apiKey", "xyz")
    .getOrCreate

  it("a") {

    spark.sql(
      s"create table search.default.test (id int, name string) " +
        s"using azsearch " +
        s"options (" +
        s"endPoint 'abc'," +
        s"apiKey 'xyz'" +
        s")"
    )

    val x = 2
  }
}
