package io.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.SearchFieldDataType
import io.github.jarol.azure.search.spark.sql.connector.models._
import io.github.jarol.azure.search.spark.sql.connector.{SearchITSpec, SparkSpec}
import io.github.jarol.azure.search.spark.sql.connector.core.Constants
import org.apache.spark.sql.SaveMode

import java.lang.{Double => JDouble, Long => JLong}
import java.sql.{Date, Timestamp}
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalTime}
import scala.reflect.runtime.universe.TypeTag

class WriteSpec
  extends SearchITSpec
    with SparkSpec {

  private lazy val atomicBeansIndex = "write-atomic-beans"
  private lazy val collectionBeansIndex = "write-collection-beans"
  private lazy val complexBeansIndex = "write-complex-beans"

  /**
   * Write some documents to a Search index
   * @param index index name
   * @param documents data to write
   * @param columnNames names to use for Dataframe columns
   * @param extraOptions additional write options
   * @param mode write [[SaveMode]]
   */

  private def writeUsingDataSource[T <: ITDocument with Product: TypeTag](
                                                                           index: String,
                                                                           documents: Seq[T],
                                                                           columnNames: Option[Seq[String]],
                                                                           extraOptions: Option[Map[String, String]],
                                                                           mode: SaveMode = SaveMode.Append
                                                                         ): Unit = {

    // Create dataFrame
    val dataFrame = columnNames.map {
      toDF(documents, _)
    }.getOrElse(toDF(documents))

    // Set up the writer
    val basicWriter = dataFrame.write.format(Constants.DATASOURCE_NAME)
      .options(optionsForAuthAndIndex(index))
      .option(WriteConfig.FIELD_OPTIONS_PREFIX + SearchFieldCreationOptions.KEY_FIELD_CONFIG, "id")

    // Add extra options, if needed
    extraOptions
      .map(basicWriter.options)
      .getOrElse(basicWriter)
      .mode(mode)
      .save()

    // Wait for some time in order to ensure test consistency
    Thread.sleep(5000)
  }

  /**
   * Assert the proper decoding behavior for values that are written to a Search index
   * @param value value to write
   * @param colName column name for value to write
   * @param expectedDecoding expected decoding function
   * @tparam TInput input type
   * @tparam TOutput output type (should have an implicit [[PropertyDeserializer]] in scope)
   */

  private def assertWriteBehaviorFor[TInput: TypeTag, TOutput: PropertyDeserializer](
                                                                                      value: TInput,
                                                                                      colName: String,
                                                                                      expectedDecoding: TInput => TOutput
                                                                                    ): Unit = {

    // Create and write a simple document
    val expected = PairBean.apply[TInput](value)
    writeUsingDataSource(atomicBeansIndex, Seq(expected), Some(Seq("id", colName)), None)

    // Retrieve the document using standard Java client API
    val output = readAllDocumentsAs[PairBean[TOutput]](atomicBeansIndex)(PairBean.deserializerFor[TOutput](colName))
      .collectFirst {
        case bean if bean.id.equals(expected.id) => bean
      }

    output shouldBe defined
    val actual = output.get
    actual.id shouldBe expected.id
    actual.value shouldBe expected.value.map(expectedDecoding)
  }

  /**
   * Assert that a Spark internal array has been properly written to a Search index
   * @param input input document
   * @param expectedSearchType expected Search collection type
   * @tparam T Java/Scala array inner type
   */

  private def assertWriteArrayBehaviorFor[T: PropertyDeserializer: TypeTag](
                                                                             input: CollectionBean[T],
                                                                             expectedSearchType: SearchFieldDataType
                                                                           ): Unit = {

    // Drop index and write the document
    dropIndexIfExists(collectionBeansIndex, sleep = true)
    writeUsingDataSource(collectionBeansIndex, Seq(input), None, None)
    indexExists(collectionBeansIndex) shouldBe true

    // Assert Search collection type
    val maybeArrayType = getIndexFields(collectionBeansIndex).collectFirst {
      case (k, field) if k.equalsIgnoreCase("array") => field.getType
    }

    maybeArrayType shouldBe defined
    maybeArrayType.get shouldBe SearchFieldDataType.collection(expectedSearchType)

    // Read documents and run assertion
    val documents = readAllDocumentsAs[CollectionBean[T]](collectionBeansIndex)(CollectionBean.deserializerFor[T])
    documents should have size 1
    val actual = documents.head
    actual.id shouldBe input.id
    (actual.array, input.array) match {
      case (Some(a), Some(b)) => a should contain theSameElementsAs b
      case (None, None) =>
      case _ => fail("Unexpected case (one of the arrays is empty, while the other doesn't)")
    }
  }

  /**
   * Asser that a Spark internal row has been properly written as a sub document to a Search index
   * @param subDocument sub document
   * @param expectedSearchFieldType expected Search field type for the sub document field
   * @tparam T sub document type
   */

  private def assertWriteComplexBehavior[T <: Product: PropertyDeserializer: TypeTag](
                                                                                       subDocument: T,
                                                                                       expectedSearchFieldType: SearchFieldDataType
                                                                                     ): Unit = {

    // Drop index and write data
    dropIndexIfExists(complexBeansIndex, sleep = true)
    val input: PairBean[T] = PairBean[T](subDocument)
    writeUsingDataSource(complexBeansIndex, Seq(input), None, None)

    // Assertion for sub document Search type
    val maybeType = getIndexFields(complexBeansIndex).collectFirst {
      case (k, field) if k.equals("value") && field.getType.equals(expectedSearchFieldType) =>
        field.getType
    }

    maybeType shouldBe defined

    // Assertion on retrieved document
    val output: Seq[PairBean[T]] = readAllDocumentsAs[PairBean[T]](complexBeansIndex)(
      PairBean.deserializerFor[T]("value")
    )

    output should have size 1
    val head = output.head
    head.id shouldBe input.id
    head.value shouldBe defined
    head.value.get shouldBe subDocument
  }

  describe("Search dataSource") {
    describe(SHOULD) {
      describe("write Spark internal") {
        describe("string as") {
          it("Search strings") {

            // Create index from schema
            createIndexFromSchemaOf[AtomicBean](atomicBeansIndex)
            assertWriteBehaviorFor[String, String]("john", "stringValue", identity)
          }
        }

        describe("numeric values as") {
          it("Search strings") {

            assertWriteBehaviorFor[Int, String](123, "stringValue", String.valueOf)
          }

          describe("Search numeric values") {
            it("of same type") {

              assertWriteBehaviorFor[JLong, JLong](12345678910L, "longValue", identity)
            }

            it("of different type") {

              assertWriteBehaviorFor[JDouble, JLong](123.456, "longValue", _.longValue())
            }
          }
        }

        describe("boolean values as") {
          it("Search strings") {

            assertWriteBehaviorFor[Boolean, String](false, "stringValue", String.valueOf)
          }

          it("Search booleans") {

            assertWriteBehaviorFor[Boolean, Boolean](false, "booleanValue", identity)
          }
        }

        describe("date values as") {
          it("Search strings") {

            assertWriteBehaviorFor[Date, String](
              Date.valueOf(LocalDate.now()),
              "stringValue",
              _.toLocalDate.format(DateTimeFormatter.ISO_LOCAL_DATE)
            )
          }

          it("Search datetimeOffset") {

            assertWriteBehaviorFor[Date, Timestamp](
              Date.valueOf(LocalDate.now()),
              "timestampValue",
              d => Timestamp.from(d.toLocalDate.atTime(LocalTime.MIDNIGHT).toInstant(Constants.UTC_OFFSET))
            )
          }
        }

        describe("timestamp values as") {
          it("Search strings") {

            assertWriteBehaviorFor[Timestamp, String](
              Timestamp.from(Instant.now()),
              "stringValue",
              _.toInstant.atOffset(Constants.UTC_OFFSET).format(Constants.DATETIME_OFFSET_FORMATTER)
            )
          }

          it("Search datetimeOffset") {

            assertWriteBehaviorFor[Timestamp, Timestamp](
              Timestamp.from(Instant.now()),
              "timestampValue",
              identity
            )
          }
        }

        describe("arrays as Search collections of") {
          it("simple types") {

            val expected = CollectionBean[String]("hello", Some(Seq("john", "doe")))
            assertWriteArrayBehaviorFor[String](expected, SearchFieldDataType.STRING)
          }

          it("complex types") {

            val expected = CollectionBean[SimpleBean](
              "hello",
              Some(
                Seq(
                  SimpleBean("john", Some(LocalDate.now())),
                  SimpleBean("jane", Some(LocalDate.now().plusDays(1)))
                )
              )
            )

            assertWriteArrayBehaviorFor[SimpleBean](expected, SearchFieldDataType.COMPLEX)
          }

          it("geopoints") {

            val expected = CollectionBean[GeoBean](
              "hello",
              Some(
                Seq(
                  GeoBean(Seq(3.14, 4.56)),
                  GeoBean(Seq(6.57, 7.89))
                )
              )
            )

            assertWriteArrayBehaviorFor[GeoBean](expected, SearchFieldDataType.GEOGRAPHY_POINT)
            dropIndexIfExists(collectionBeansIndex, sleep = false)
          }
        }

        describe("internal Rows as") {
          it("sub documents") {

            assertWriteComplexBehavior[SimpleBean](
              SimpleBean("john", Some(LocalDate.now())),
              SearchFieldDataType.COMPLEX
            )
          }

          it("geo points") {

            assertWriteComplexBehavior[GeoBean](
              GeoBean(Seq(3.14, 4.56)),
              SearchFieldDataType.GEOGRAPHY_POINT
            )

            dropIndexIfExists(complexBeansIndex, sleep = false)
          }
        }
      }

      it("overwrite an existing index") {

        val overwriteIndexName = "overwrite-test-index"
        val previousDocuments: Seq[SimpleBean] = Seq(
          SimpleBean("hello", Some(LocalDate.now()))
        )

        writeUsingDataSource(overwriteIndexName, previousDocuments, None, None)
        indexExists(overwriteIndexName) shouldBe true
        assertMatchBetweenSchemaAndIndex[SimpleBean](overwriteIndexName)

        val actualDocuments: Seq[PairBean[Int]] = Seq(
          PairBean(1),
          PairBean(2)
        )

        writeUsingDataSource(overwriteIndexName, actualDocuments, None, None, SaveMode.Overwrite)
        assertMatchBetweenSchemaAndIndex[PairBean[Int]](overwriteIndexName)
        dropIndexIfExists(overwriteIndexName, sleep = false)
      }
    }
  }
}
