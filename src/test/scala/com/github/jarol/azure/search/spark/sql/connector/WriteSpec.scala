package com.github.jarol.azure.search.spark.sql.connector

import com.azure.search.documents.models.IndexActionType
import com.github.jarol.azure.search.spark.sql.connector.config.{IOConfig, WriteConfig}
import org.apache.spark.sql.SaveMode

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}

class WriteSpec
  extends SparkSpec {

  it("a") {

    val df = toDF(
      Seq(
        Person("luca", Date.valueOf(LocalDate.now()), Timestamp.valueOf(LocalDateTime.now()), Some(32))
      )
    )

    df.write.format(SearchTableProvider.SHORT_NAME)
      .option(IOConfig.INDEX_CONFIG, "people")
      .option(WriteConfig.ACTION_CONFIG, IndexActionType.UPLOAD.name())
      .mode(SaveMode.Append)
      .save()
  }
}
