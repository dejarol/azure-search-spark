package com.github.jarol.azure.search.spark.sql.connector

import com.azure.search.documents.models.IndexActionType
import com.github.jarol.azure.search.spark.sql.connector.core.Constants
import com.github.jarol.azure.search.spark.sql.connector.core.schema.SearchFieldFeature
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
  private lazy val featuresIndex = "write-features-index"
  private lazy val atomicBeansIndex = "write-atomic-beans"

  override protected lazy val itSearchIndexNames: Seq[String] = Seq(
    simpleBeansIndex,
    actionTypeIndex,
    featuresIndex,
    atomicBeansIndex
  )

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

  /**
   * Assert that a field has been properly enabled/disabled when creating a new index
   * @param indexName index name
   * @param names name of fields to enable
   * @param feature feature to enable
   */

  private def assertFeatureEnablingOnIndex(
                                            indexName: String,
                                            names: Seq[String],
                                            feature: SearchFieldFeature
                                          ): Unit = {

    // Separate expected enabled fields from expected disabled fields
    val (expectedEnabled, expectedNotEnabled) = getIndexFields(indexName)
      .partition {
        p => names.exists {
          _.equalsIgnoreCase(p.getName)
      }
    }

    // Assertion for expected enabled fields
    forAll(expectedEnabled) {
      field => feature.isEnabledOnField(field) shouldBe true
    }

    // Assertion for expected disabled fields
    forAll(expectedNotEnabled) {
      field => feature.isDisabledOnField(field) shouldBe true
    }
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
      describe("create an index (if it does not exist)") {
        it("with as many fields as many columns") {

          val input: Seq[SimpleBean] = Seq(
            SimpleBean("hello", Some(LocalDate.now()))
          )

          dropIndexIfExists(simpleBeansIndex, sleep = true)
          indexExists(simpleBeansIndex) shouldBe false
          writeToIndex(simpleBeansIndex, input, None)

          val expectedSearchFieldNames = schemaOfCaseClass[SimpleBean].fields.map(_.name)
          val actualFieldNames = getIndexFields(simpleBeansIndex).map(_.getName)
          actualFieldNames should contain theSameElementsAs expectedSearchFieldNames
          dropIndexIfExists(simpleBeansIndex, sleep = false)
        }

        it("not including the column used for index action type") {

          val indexActionColumn = "action"
          val documents: Seq[ActionTypeBean] = Seq(
            ActionTypeBean("hello", Some(1), IndexActionType.UPLOAD)
          )

          dropIndexIfExists(actionTypeIndex, sleep = true)
          indexExists(actionTypeIndex) shouldBe false

          val extraOptions = Map(
            WriteConfig.INDEX_ACTION_COLUMN_CONFIG -> indexActionColumn
          )

          writeToIndex(actionTypeIndex, documents, Some(extraOptions))
          indexExists(actionTypeIndex) shouldBe true
          val expectedSearchFieldNames = schemaOfCaseClass[ActionTypeBean].fields.collect {
            case sf if !sf.name.equalsIgnoreCase(indexActionColumn) => sf.name
          }

          val actualFieldNames = getIndexFields(actionTypeIndex).map(_.getName)
          actualFieldNames should contain theSameElementsAs expectedSearchFieldNames
          dropIndexIfExists(actionTypeIndex, sleep = false)
        }

        describe("allowing the user to enable field properties, like being") {

          lazy val documents = Seq(
            FeaturesBean("hello", Some("DISCOUNT"), Some(0))
          )

          it("facetable") {

            val facetableFields = Seq("category")
            val extraOptions = Map(
              WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.FACETABLE_FIELDS -> facetableFields.mkString(",")
            )

            dropIndexIfExists(featuresIndex, sleep = true)
            writeToIndex(featuresIndex, documents, Some(extraOptions))
            assertFeatureEnablingOnIndex(featuresIndex, facetableFields, SearchFieldFeature.FACETABLE)
          }

          it("filterable") {

            val filterableFields = Seq("category")
            val extraOptions = Map(
              WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.FILTERABLE_FIELDS -> filterableFields.mkString(",")
            )

            dropIndexIfExists(featuresIndex, sleep = true)
            writeToIndex(featuresIndex, documents, Some(extraOptions))
            assertFeatureEnablingOnIndex(featuresIndex, filterableFields, SearchFieldFeature.FILTERABLE)
          }

          it("hidden") {

            val hiddenFields = Seq("category", "level")
            val extraOptions = Map(
              WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.HIDDEN_FIELDS -> hiddenFields.mkString(",")
            )

            dropIndexIfExists(featuresIndex, sleep = true)
            writeToIndex(featuresIndex, documents, Some(extraOptions))
            assertFeatureEnablingOnIndex(featuresIndex, hiddenFields, SearchFieldFeature.HIDDEN)
          }

          it("searchable") {

            val searchableFields = Seq("category")
            val extraOptions = Map(
              WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.SEARCHABLE_FIELDS -> searchableFields.mkString(",")
            )

            dropIndexIfExists(featuresIndex, sleep = true)
            writeToIndex(featuresIndex, documents, Some(extraOptions))
            assertFeatureEnablingOnIndex(featuresIndex, searchableFields, SearchFieldFeature.SEARCHABLE)
          }

          it("sortable") {

            val sortableFields = Seq("level")
            val extraOptions = Map(
              WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.SORTABLE_FIELDS -> sortableFields.mkString(",")
            )

            dropIndexIfExists(featuresIndex, sleep = true)
            writeToIndex(featuresIndex, documents, Some(extraOptions))
            assertFeatureEnablingOnIndex(featuresIndex, sortableFields, SearchFieldFeature.SORTABLE)
          }
        }
      }

      describe("write Spark internal") {

        describe("string as") {
          it("Search strings") {

            // Create index from schema
            dropIndexIfExists(simpleBeansIndex, sleep = true)
            dropIndexIfExists(actionTypeIndex, sleep = true)
            dropIndexIfExists(featuresIndex, sleep = true)
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

            // TODO
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
