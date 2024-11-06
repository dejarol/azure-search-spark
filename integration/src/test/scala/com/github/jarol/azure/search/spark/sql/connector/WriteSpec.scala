package com.github.jarol.azure.search.spark.sql.connector

import com.github.jarol.azure.search.spark.sql.connector.core.Constants
import com.github.jarol.azure.search.spark.sql.connector.models._
import com.github.jarol.azure.search.spark.sql.connector.write.WriteConfig
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalatest.Inspectors

import java.lang.{Double => JDouble, Long => JLong}
import java.sql.{Date, Timestamp}
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalTime}
import scala.reflect.runtime.universe.TypeTag

class WriteSpec
  extends SearchSparkIntegrationSpec
    with Inspectors {

  private lazy val simpleBeansIndex = "write-simple-beans"
  private lazy val actionTypeIndex = "write-action-type-beans"
  private lazy val atomicBeansIndex = "write-atomic-beans"
  private lazy val collectionBeansIndex = "write-atomic-beans"

  /**
   * Write a Dataframe to a Search index
   * @param index index
   * @param documents documents to write
   * @param extraOptions extra writer options
   * @param sleep whether to wait for some time after document write
   */

  private def writeToIndex(
                            index: String,
                            documents: DataFrame,
                            extraOptions: Option[Map[String, String]],
                            sleep: Boolean
                          ): Unit = {

    // Set up the writer
    val basicWriter = documents.write.format(Constants.DATASOURCE_NAME)
      .options(optionsForAuthAndIndex(index))
      .option(WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.KEY_FIELD, "id")

    // Add extra options, if needed
    extraOptions
      .map(basicWriter.options)
      .getOrElse(basicWriter)
      .mode(SaveMode.Append)
      .save()

    // Wait for some time in order to ensure test consistency
    if (sleep) {
      Thread.sleep(5000)
    }
  }

  /**
   * Write some documents to a Search index
   * @param index index name
   * @param documents data to write
   * @param extraOptions additional write options
   */

  private def writeToIndex[T <: AbstractITDocument with Product: TypeTag](
                                                                           index: String,
                                                                           documents: Seq[T],
                                                                           extraOptions: Option[Map[String, String]]
                                                                         ): Unit = {

    writeToIndex(
      index,
      toDF(documents),
      extraOptions,
      sleep = false
    )
  }

  /**
   * Write some documents to a Search index
   * @param index index name
   * @param documents data to write
   * @param columnNames names to use for Dataframe columns
   * @param extraOptions additional write options
   */

  private def writeToIndex[T <: AbstractITDocument with Product: TypeTag](
                                                                           index: String,
                                                                           documents: Seq[T],
                                                                           columnNames: Seq[String],
                                                                           extraOptions: Option[Map[String, String]]
                                                                         ): Unit = {

    writeToIndex(
      index,
      toDF(documents, columnNames),
      extraOptions,
      sleep = true
    )
  }

  private def assertCorrectDecodingFor[TInput: TypeTag, TOutput: PropertyDeserializer](
                                                                                        value: TInput,
                                                                                        colName: String,
                                                                                        expectedDecoding: TInput => TOutput
                                                                                      ): Unit = {

    // Create and write a simple document
    val expected = PairBean.apply[TInput](value)
    writeToIndex(atomicBeansIndex, Seq(expected), Seq("id", colName), None)

    // Retrieve the document using standard Java client API
    val output = readDocumentsAs[PairBean[TOutput]](atomicBeansIndex)(PairBean.deserializerFor[TOutput](colName))
      .collectFirst {
        case bean if bean.id.equals(expected.id) => bean
      }

    output shouldBe defined
    val actual = output.get
    actual.id shouldBe expected.id
    actual.value shouldBe expected.value.map(expectedDecoding)
  }

  describe("Search dataSource") {
    describe(SHOULD) {
      describe("write Spark internal") {

        describe("string as") {
          it("Search strings") {

            // Clean up indexes
            dropIndexIfExists(simpleBeansIndex, sleep = true)
            dropIndexIfExists(actionTypeIndex, sleep = true)
            dropIndexIfExists(atomicBeansIndex, sleep = true)

            // Create index from schema
            createIndexFromSchemaOf[AtomicBean](atomicBeansIndex)
            assertCorrectDecodingFor[String, String]("john", "stringValue", identity)
          }
        }

        describe("numeric values as") {
          it("Search strings") {

            assertCorrectDecodingFor[Int, String](123, "stringValue", String.valueOf)
          }

          describe("Search numeric values") {
            it("of same type") {

              assertCorrectDecodingFor[JLong, JLong](12345678910L, "longValue", identity)
            }

            it("of different type") {

              assertCorrectDecodingFor[JDouble, JLong](123.456, "longValue", _.longValue())
            }
          }
        }

        describe("boolean values as") {
          it("Search strings") {

            assertCorrectDecodingFor[Boolean, String](false, "stringValue", String.valueOf)
          }

          it("Search booleans") {

            assertCorrectDecodingFor[Boolean, Boolean](false, "booleanValue", identity)
          }
        }

        describe("date values as") {
          it("Search strings") {

            assertCorrectDecodingFor[Date, String](
              Date.valueOf(LocalDate.now()),
              "stringValue",
              _.toLocalDate.format(DateTimeFormatter.ISO_LOCAL_DATE)
            )
          }

          it("Search datetimeOffset") {

            assertCorrectDecodingFor[Date, Timestamp](
              Date.valueOf(LocalDate.now()),
              "timestampValue",
              d => Timestamp.from(d.toLocalDate.atTime(LocalTime.MIDNIGHT).toInstant(Constants.UTC_OFFSET))
            )
          }
        }

        describe("timestamp values as") {
          it("Search strings") {

            assertCorrectDecodingFor[Timestamp, String](
              Timestamp.from(Instant.now()),
              "stringValue",
              _.toInstant.atOffset(Constants.UTC_OFFSET).format(Constants.DATETIME_OFFSET_FORMATTER)
            )
          }

          it("Search datetimeOffset") {

            assertCorrectDecodingFor[Timestamp, Timestamp](
              Timestamp.from(Instant.now()),
              "timestampValue",
              identity
            )
          }
        }

        describe("arrays as Search collections of") {
          it("simple types") {

            val collectionBean = CollectionBean[String]("hello", Some(Seq("john", "doe")))
            writeToIndex[CollectionBean[String]]()
          }

          it("complex types") {

            // TODO
          }

          it("geopoints") {

            // TODO

          }
        }
      }
    }
  }
}
