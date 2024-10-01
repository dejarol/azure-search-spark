package com.github.jarol.azure.search.spark.sql.connector

import com.azure.search.documents.models.IndexActionType
import com.github.jarol.azure.search.spark.sql.connector.core.SparkSpec
import com.github.jarol.azure.search.spark.sql.connector.core.config.IOConfig
import com.github.jarol.azure.search.spark.sql.connector.write.WriteConfig
import org.apache.spark.sql.SaveMode

import java.sql.Date
import java.time.LocalDateTime
import java.time.temporal.{ChronoUnit, TemporalAdjusters}
import java.util.UUID

class WriteSpec
  extends SparkSpec {

  import WriteSpec._

  ignore("a") {

    val sparkPartitionID = "spark_partition_id"
    val models = createModels(1, 3)
    /*
    val models = Seq(
      Model(
        "1",
        Date.valueOf(LocalDate.now()),
        None,
        Some(
          Seq(
            Reference(
              "2",
              2,
              None
            )
          )
        )
      )
    )

     */
    val df = spark.createDataFrame(
        spark.sparkContext.parallelize(models, 3)
    )

    val options = Map(
      IOConfig.API_KEY_CONFIG -> "jWFM1tzIjG8pEtkOs437CoY1xqMXXPJ8iFiiwfd9BAAzSeAprBmR",
      IOConfig.END_POINT_CONFIG -> "https://searchsparkd01cs01.search.windows.net",
      IOConfig.INDEX_CONFIG -> "models"
    )

    /*
    val converters = WriteMappingSupplier.get(
      df.schema,
      WriteConfig(options, Map.empty[String, String]).getSearchIndexFields,
      "models"
    ).right.get

    val x = InternalRowToSearchDocumentConverter(converters)
    val xx = RowEncoder.apply(df.schema).createSerializer()
    val rows = df.collect()
    rows.foreach {
      r =>
        Try {
          val internal = xx.apply(r)
          x.apply(internal)
        } match {
          case Failure(exception) =>
            println(s"Error for row with id: ${r.getAs[String]("id")}. " +
              s"Class: ${exception.getClass}, " +
              s"message: ${exception.getMessage}")

            val a = 1

          case Success(value) => ???
        }
    }

     */

    df.write.format(SearchTableProvider.SHORT_NAME)
      .options(options)
      .option(WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.KEY_FIELD, "id")
      .option(WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.FILTERABLE_FIELDS, s"date,$sparkPartitionID")
      .option(WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.FACETABLE_FIELDS, s"date,$sparkPartitionID")
      .option(WriteConfig.ACTION_CONFIG, IndexActionType.MERGE_OR_UPLOAD.name())
      .mode(SaveMode.Append)
      .save()

  }
}

object WriteSpec {

  def createModels(n: Int, p: Int): Seq[Model] = {

    val now = LocalDateTime.now()
    val twoMonthsAgo = now.minusMonths(2).`with`(TemporalAdjusters.firstDayOfMonth())
    val deltaInMinutes = ChronoUnit.MINUTES.between(twoMonthsAgo, now)

    (0 until n).map {
      _ =>

        val date = twoMonthsAgo.plus((math.random() * deltaInMinutes).toLong, ChronoUnit.MINUTES)
        val address = if (math.random() >= 0.95) {
          None
        } else {

          val location = if (math.random() >= 0.95) {
            None
          } else {
            Some(
              Location("Point", Seq(math.random() * 90, math.random() * 90))
            )
          }
          Some(
            Address(
              "ROME",
              util.Random.nextInt(n),
              location
            )
          )
        }

        val references = if (math.random() >= 0.95) {
          None
        } else {
          Some(
            (1 to util.Random.nextInt(3)).map {
              _ =>
                Reference(
                  UUID.randomUUID().toString.toUpperCase,
                  util.Random.nextInt(100),
                  if (math.random() >= 0.95) None else Some(
                    Date.valueOf(
                      twoMonthsAgo.plus((deltaInMinutes * math.random()).toLong, ChronoUnit.MINUTES)
                        .toLocalDate
                    )
                  )
                )
            }
          )
        }

        Model(
          UUID.randomUUID().toString.toUpperCase,
          Date.valueOf(date.toLocalDate),
          address,
          references,
          util.Random.nextInt(p)
        )
    }
  }

  case class Location(
                     `type`: String,
                     coordinates: Seq[java.lang.Double]
                     )

  case class Reference(
                        id: String,
                        value: Integer,
                        startDate: Option[Date]
                       )

  case class Address(
                      city: String,
                      zipCode: Int,
                      location: Option[Location]
                    )

  case class Model(
                    id: String,
                    date: Date,
                    address: Option[Address],
                    references: Option[Seq[Reference]],
                    partitionId: Integer
                  )
}
