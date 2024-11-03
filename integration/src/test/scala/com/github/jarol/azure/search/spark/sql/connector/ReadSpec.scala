package com.github.jarol.azure.search.spark.sql.connector

import com.github.jarol.azure.search.spark.sql.connector.core.{Constants, FieldFactory, IndexDoesNotExistException, JavaScalaConverters}
import com.github.jarol.azure.search.spark.sql.connector.models._
import com.github.jarol.azure.search.spark.sql.connector.read.ReadConfig
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.scalatest.{BeforeAndAfterAll, Inspectors}

import java.sql.{Date, Timestamp}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, OffsetDateTime}

class ReadSpec
  extends SearchSparkIntegrationSpec
    with FieldFactory
    with Inspectors
    with BeforeAndAfterAll {

  private lazy val simpleBeansIndex = "read-simple-beans"
  private lazy val atomicBeansIndex = "read-atomic-beans"
  private lazy val collectionBeansIndex = "read-collection-beans"

  override protected lazy val itSearchIndexNames: Seq[String] = Seq(
    simpleBeansIndex,
    atomicBeansIndex,
    collectionBeansIndex
  )

  /**
   * Write a collection of documents to an index
   * @param indexName index name
   * @param documents documents
   * @tparam T document type (an implicit [[DocumentSerializer]] for this type is expected to be on scope)
   */

  private final def writeDocuments[T: DocumentSerializer](
                                                           indexName: String,
                                                           documents: Seq[T]
                                                         ): Unit = {

    SearchTestUtils.writeDocuments[T](
      getSearchClient(indexName),
      JavaScalaConverters.seqToList(documents),
      implicitly[DocumentSerializer[T]]
    )

    // Wait for some seconds in order to ensure test consistency
    Thread.sleep(5000)
  }

  /**
   * Read data from a target index
   * @param name index name
   * @param filter filter to apply on index data
   * @param select list of fields to select
   * @param schema optional read schema
   * @return index data
   */

  private def readIndex(
                         name: String,
                         filter: Option[String],
                         select: Option[Seq[String]],
                         schema: Option[StructType]
                       ): DataFrame = {

    // Set extra options
    val extraOptions = Map(
      ReadConfig.FILTER_CONFIG -> filter,
      ReadConfig.SELECT_CONFIG -> select.map(_.mkString(","))
    ).collect {
      case (k, Some(v)) => (k, v)
    }

    // Set up a reader
    val reader = spark.read.format(Constants.DATASOURCE_NAME)
      .options(optionsForAuthAndIndex(name))
      .options(extraOptions)

    // Optionally apply schema
    schema match {
      case Some(value) => reader.schema(value).load(name)
      case None => reader.load(name)
    }
  }

  /**
   * Zip together rows and numeric beans, sorting both by id
   * @param rows rows
   * @param beans beans
   * @return a collection of tuples
   */

  private def zipRowsAndBeans[D <: ITDocument](
                                                rows: Seq[Row],
                                                beans: Seq[D]
                                              ): Seq[(Row, D)] = {

    rows
      .sortBy(_.getAs[String]("id"))
      .zip(beans.sortBy(_.id))
  }

  /**
   * Assert that an atomic value has been encoded correctly
   * @param output output row
   * @param input input row
   * @param colName column name to test
   * @param encodingFunction expected encoding function
   * @tparam TOutput output type
   */

  private def assertAtomicBeanEncoding[TOutput](
                                                 output: Row,
                                                 input: AtomicBean,
                                                 colName: String,
                                                 encodingFunction: AtomicBean => Option[TOutput]
                                               ): Unit = {

    output.getAsOpt[TOutput](colName) shouldBe encodingFunction(input)
  }

  describe("Search dataSource") {
    describe(SHOULD) {
      describe("throw an exception when") {
        it("target index does not exist") {

          val indexName = "non-existing"
          dropIndexIfExists(indexName, sleep = true)
          indexExists(indexName) shouldBe false
          a [IndexDoesNotExistException] shouldBe thrownBy {
            readIndex(indexName, None, None, None)
          }
        }
      }

      describe("read documents") {
        it("that match a filter") {

          val id = "hello"
          val input = Seq(
            SimpleBean(id, Some(LocalDate.now())),
            SimpleBean("world", Some(LocalDate.now().plusDays(1)))
          )

          dropIndexIfExists(simpleBeansIndex, sleep = true)
          indexExists(simpleBeansIndex) shouldBe false
          createIndexFromSchemaOf[SimpleBean](simpleBeansIndex)
          writeDocuments[SimpleBean](simpleBeansIndex, input)
          val df = readIndex(simpleBeansIndex, Some(s"id eq '$id'"), None, Some(schemaOfCaseClass[SimpleBean]))

          val output = toSeqOf[SimpleBean](df)
          output should have size 1
          output.head.id shouldBe id
        }

        it("selecting some fields") {

          dropIndexIfExists(simpleBeansIndex, sleep = true)
          indexExists(simpleBeansIndex) shouldBe false
          val input = Seq(
            SimpleBean("hello", Some(LocalDate.now())),
            SimpleBean("world", Some(LocalDate.now().plusDays(1)))
          )

          createIndexFromSchemaOf[SimpleBean](simpleBeansIndex)
          writeDocuments[SimpleBean](simpleBeansIndex, input)
          val select = Seq("id", "date")
          val df = readIndex(simpleBeansIndex, None, Some(select), None)
          df.count() shouldBe input.size
          df.columns should contain theSameElementsAs select
        }

        describe("translating") {

          lazy val notNullBean = AtomicBean.from("hello", Some(1), Some(123), Some(3.45), Some(false), Some(OffsetDateTime.now(Constants.UTC_OFFSET)))
          lazy val nullBean = AtomicBean.from("world", None, None, None, None, None)
          lazy val numericSamples = Seq(notNullBean, nullBean)

          describe("numeric values as") {
            it("strings") {

              dropIndexIfExists(atomicBeansIndex, sleep = true)
              indexExists(atomicBeansIndex) shouldBe false
              createIndexFromSchemaOf[AtomicBean](atomicBeansIndex)
              writeDocuments[AtomicBean](atomicBeansIndex, numericSamples)

              // Create a schema with all string fields
              val schemaRead = schemaOfCaseClass[AtomicBean].map {
                sf => if (!sf.name.equals("id")) sf.copy(dataType = DataTypes.StringType) else sf
              }

              // Read data and do assertions
              val rows: Seq[Row] = readIndex(atomicBeansIndex, None, None, Some(StructType(schemaRead))).collect()
              rows should have size numericSamples.size
              forAll(zipRowsAndBeans(rows, numericSamples)) {
                case (row, sample) =>
                  assertAtomicBeanEncoding[String](row, sample, "intValue", _.intValue.map(String.valueOf))
                  assertAtomicBeanEncoding[String](row, sample, "longValue", _.longValue.map(String.valueOf))
                  assertAtomicBeanEncoding[String](row, sample, "doubleValue", _.doubleValue.map(String.valueOf))
              }
            }

            it("numeric values of different type") {

              val schemaRead = createStructType(
                createStructField("id", DataTypes.StringType),
                createStructField("intValue", DataTypes.DoubleType),
                createStructField("longValue", DataTypes.DoubleType)
              )

              val rows = readIndex(atomicBeansIndex, None, None, Some(schemaRead)).collect()
              rows should have size numericSamples.size
              forAll(zipRowsAndBeans(rows, numericSamples)) {
                case (row, bean) =>
                  assertAtomicBeanEncoding[Double](row, bean, "intValue", _.intValue.map(_.doubleValue()))
                  assertAtomicBeanEncoding[Double](row, bean, "longValue", _.longValue.map(_.doubleValue()))
              }
            }
          }

          describe("boolean values as") {
            it("booleans") {

              val schemaRead = createStructType(
                createStructField("id", DataTypes.StringType),
                createStructField("booleanValue", DataTypes.BooleanType)
              )

              val rows = readIndex(atomicBeansIndex, None, None, Some(schemaRead)).collect()
              rows should have size numericSamples.size
              forAll(zipRowsAndBeans(rows, numericSamples)) {
                case (r, s) =>
                  assertAtomicBeanEncoding[Boolean](r, s, "booleanValue", _.booleanValue)
              }
            }

            it("strings") {

              val schemaRead = createStructType(
                createStructField("id", DataTypes.StringType),
                createStructField("booleanValue", DataTypes.StringType)
              )

              val rows = readIndex(atomicBeansIndex, None, None, Some(schemaRead)).collect()
              rows should have size numericSamples.size
              forAll(zipRowsAndBeans(rows, numericSamples)) {
                case (r, s) =>
                  assertAtomicBeanEncoding[String](r, s, "booleanValue", _.booleanValue.map(String.valueOf))
              }
            }
          }

          describe("datetimeoffset values as") {
            it("dates") {

              val schemaRead = createStructType(
                createStructField("id", DataTypes.StringType),
                createStructField("timestampValue", DataTypes.DateType)
              )

              val rows = readIndex(atomicBeansIndex, None, None, Some(schemaRead)).collect()
              rows should have size numericSamples.size
              forAll(zipRowsAndBeans(rows, numericSamples)) {
                case (r, s) =>
                  assertAtomicBeanEncoding[Date](r, s, "timestampValue", _.timestampValue.map {
                    t => Date.valueOf(t.toInstant.atOffset(Constants.UTC_OFFSET).format(DateTimeFormatter.ISO_LOCAL_DATE))
                  })
              }
            }

            it("timestamps") {

              val schemaRead = createStructType(
                createStructField("id", DataTypes.StringType),
                createStructField("timestampValue", DataTypes.TimestampType)
              )

              val rows = readIndex(atomicBeansIndex, None, None, Some(schemaRead)).collect()
              rows should have size numericSamples.size
              forAll(zipRowsAndBeans(rows, numericSamples)) {
                case (r, s) =>
                  assertAtomicBeanEncoding[Timestamp](r, s, "timestampValue", _.timestampValue)
              }
            }

            it("strings") {

              val schemaRead = createStructType(
                createStructField("id", DataTypes.StringType),
                createStructField("timestampValue", DataTypes.StringType)
              )

              val rows = readIndex(atomicBeansIndex, None, None, Some(schemaRead)).collect()
              rows should have size numericSamples.size
              forAll(zipRowsAndBeans(rows, numericSamples)) {
                case (r, s) =>
                  assertAtomicBeanEncoding[String](r, s, "timestampValue", _.timestampValue.map {
                    _.toInstant.atZone(Constants.UTC_OFFSET)
                      .format(Constants.DATETIME_OFFSET_FORMATTER)
                  })
              }
            }
          }
        }

        describe("containing") {
          describe("collections of") {
            it("simple types") {

              dropIndexIfExists(collectionBeansIndex, sleep = true)
              indexExists(collectionBeansIndex) shouldBe false
              val samples = Seq(
                CollectionBean[String]("hello", Some(Seq("world", "John"))),
                CollectionBean[String]("world", None)
              )

              createIndexFromSchemaOf[CollectionBean[String]](collectionBeansIndex)
              writeDocuments[CollectionBean[String]](collectionBeansIndex, samples)(
                CollectionBean.serializerFor[String]
              )
              val rows = readIndex(collectionBeansIndex, None, None, None).collect()
              rows should have size samples.size
              forAll(zipRowsAndBeans(rows, samples)) {
                case (r, s) =>
                  r.getAs[Seq[String]]("array") should contain theSameElementsAs s.array.getOrElse(Seq.empty)
              }
            }

            it("complex types") {

              dropIndexIfExists(collectionBeansIndex, sleep = true)
              indexExists(collectionBeansIndex) shouldBe false
              val samples: Seq[CollectionBean[ActionTypeBean]] = Seq(
                CollectionBean("hello", Some(
                  Seq(
                    ActionTypeBean("john", Some(1), "action"),
                    ActionTypeBean("jane", None, "delete")
                  )
                )),
                CollectionBean("world", None)
              )

              createIndexFromSchemaOf[CollectionBean[ActionTypeBean]](collectionBeansIndex)
              writeDocuments[CollectionBean[ActionTypeBean]](collectionBeansIndex, samples)(
                CollectionBean.serializerFor[ActionTypeBean]
              )
              val rows = toSeqOf[CollectionBean[ActionTypeBean]](readIndex(collectionBeansIndex, None, None, None))
              rows should have size samples.size
              forAll(rows.sortBy(_.id)
                .zip(samples.sortBy(_.id))
              ) {
                case (r, s) =>

                  // Null value for arrays is the empty array,
                  // so the value read from a Search index is always non-null
                  r.array shouldBe defined
                  val actual = r.array.get

                  // If input was None, actual should be empty. Otherwise, they should contain the same elements
                  s.array match {
                    case Some(value) =>
                      actual should contain theSameElementsAs value
                    case None =>
                      actual shouldBe empty
                  }
              }
            }

            it("geo points") {

              dropIndexIfExists(collectionBeansIndex, sleep = true)
              indexExists(collectionBeansIndex) shouldBe false
              val samples: Seq[CollectionBean[GeoBean]] = Seq(
                CollectionBean("hello", Some(
                  Seq(
                    GeoBean(Seq(3.14, 4.56)),
                    GeoBean(Seq(6.78, 7.89))
                  )
                )),
                CollectionBean("world", None)
              )

              createIndexFromSchemaOf[CollectionBean[GeoBean]](collectionBeansIndex)
              writeDocuments[CollectionBean[GeoBean]](collectionBeansIndex, samples)(
                CollectionBean.serializerFor[GeoBean]
              )
              val rows = toSeqOf[CollectionBean[GeoBean]](
                readIndex(collectionBeansIndex, None, None, None)
              )

              rows should have size samples.size
              forAll(rows.sortBy(_.id).zip(samples.sortBy(_.id))) {
                case (output, input) =>
                  output.array shouldBe defined
                  val actual = output.array.get

                  input.array match {
                    case Some(value) => actual should contain theSameElementsAs value
                    case None => actual shouldBe empty
                  }
              }
            }
          }
        }
      }
    }
  }
}
