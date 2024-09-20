package com.github.jarol.azure.search.spark.sql.connector

import com.azure.search.documents.models.IndexActionType
import com.github.jarol.azure.search.spark.sql.connector.core.SparkSpec
import com.github.jarol.azure.search.spark.sql.connector.core.config.IOConfig
import com.github.jarol.azure.search.spark.sql.connector.write.WriteConfig
import org.apache.spark.sql.SaveMode

import java.sql.Date
import java.time.LocalDate
import java.util.UUID

class WriteSpec
  extends SparkSpec {

  import WriteSpec._

  it("a") {

    val df = toDF(
      Seq(
        Model(
          UUID.randomUUID().toString,
          Date.valueOf(LocalDate.now()),
          Address("COMO", 22100),
          Seq(
            Reference(
              UUID.randomUUID().toString,
              Date.valueOf(
                LocalDate.now().minusDays(1)
              )
            )
          )
        )
      )
    )

    df.write.format(SearchTableProvider.SHORT_NAME)
      .option(IOConfig.API_KEY_CONFIG, "jWFM1tzIjG8pEtkOs437CoY1xqMXXPJ8iFiiwfd9BAAzSeAprBmR")
      .option(IOConfig.END_POINT_CONFIG, "https://searchsparkd01cs01.search.windows.net")
      .option(IOConfig.INDEX_CONFIG, "models")
      .option(WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.KEY_FIELD, "id")
      .option(WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.FILTERABLE_FIELDS, "id,date")
      .option(WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.FACETABLE_FIELDS, "date")
      .option(WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.SORTABLE_FIELDS, "date")
      .option(WriteConfig.ACTION_CONFIG, IndexActionType.MERGE_OR_UPLOAD.name())
      .mode(SaveMode.Append)
      .save()
  }
}

object WriteSpec {

  case class Reference(
                       id: String,
                       date: Date
                       )

  case class Address(
                      city: String,
                      zipCode: Int
                    )

  case class Model(
                    id: String,
                    date: Date,
                    address: Address,
                    references: Seq[Reference]
                  )
}
