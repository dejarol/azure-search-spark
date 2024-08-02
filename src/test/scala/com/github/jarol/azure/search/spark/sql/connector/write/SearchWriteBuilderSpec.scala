package com.github.jarol.azure.search.spark.sql.connector.write

import com.github.jarol.azure.search.spark.sql.connector.config.IOConfig
import com.github.jarol.azure.search.spark.sql.connector.{SearchTableProvider, SparkSpec}
import org.apache.spark.sql.SaveMode

class SearchWriteBuilderSpec
  extends SparkSpec {

  import SearchWriteBuilderSpec._

  it("a") {

    val df = emptyDF[Product]
    df.write.format(SearchTableProvider.SHORT_NAME)
      .option(IOConfig.INDEX_CONFIG, "aaa")
      .mode(SaveMode.Append)
      .save()
  }
}

object SearchWriteBuilderSpec {

  case class Product(encodedId: String,
                     nonEncodedId: String,
                     firstName: Option[String],
                     lastName: Option[String],
                     country: Option[String],
                     csrEmployeesCategory: Option[String],
                     position: Option[String],
                     functionBe: Option[String],
                     minHcLevel: Int,
                     urlPhoto: Option[String],
                     openManagersPopup: Boolean,
                     fullNameSortingLevel: Int)
}