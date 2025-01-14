package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.core.Constants
import com.github.jarol.azure.search.spark.sql.connector.models._
import com.github.jarol.azure.search.spark.sql.connector.{SearchITSpec, SparkSpec}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row}

import java.sql.{Date, Timestamp}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, OffsetDateTime}

class ReadSpec
  extends SearchITSpec
    with SparkSpec {

  private lazy val atomicBeansIndex = "read-atomic-beans"
  private lazy val collectionBeansIndex = "read-collection-beans"
  private lazy val pushdownPredicateIndex = "pushdown-beans"

  private lazy val now = LocalDate.now()
  private lazy val atomicBeans = Seq(
    AtomicBean(Some("john"), Some(1), Some(123), Some(3.45), Some(false), Some(OffsetDateTime.now(Constants.UTC_OFFSET))),
    AtomicBean(None, None, None, None, None, None)
  )

  private lazy val pushdownBeans: Seq[PushdownBean] = Seq(
    PushdownBean(Some("hello"), Some(1), Some(now)),
    PushdownBean(Some("world"), None, Some(now.minusDays(1))),
    PushdownBean(None, Some(2), Some(now.plusDays(1))),
    PushdownBean(None, Some(2), None),
    PushdownBean(None, Some(3), Some(now.plusDays(2))),
    PushdownBean(Some("john"), Some(3), Some(now.plusDays(2))),
    PushdownBean(Some("jane"), None, None)
  )

  /**
   * Read data from a target index
   * @param name index name
   * @param filter filter to apply on index data
   * @param select list of fields to select
   * @param schema optional read schema
   * @return index data
   */

  private def readUsingDatasource(
                                   name: String,
                                   filter: Option[String],
                                   select: Option[Seq[String]],
                                   schema: Option[StructType]
                                 ): DataFrame = {

    // Set extra options
    val extraOptions = Map(
      ReadConfig.SEARCH_OPTIONS_PREFIX + SearchOptionsConfig.FILTER_CONFIG -> filter,
      ReadConfig.SEARCH_OPTIONS_PREFIX + SearchOptionsConfig.SELECT_CONFIG -> select.map(_.mkString(","))
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

  private def asserEffectOfEncodingOn[TOutput](
                                                output: Row,
                                                input: AtomicBean,
                                                colName: String,
                                                encodingFunction: AtomicBean => Option[TOutput]
                                              ): Unit = {

    output.getAsOpt[TOutput](colName) shouldBe encodingFunction(input)
  }

  /**
   * Assert that Search datasource has pushed down a Spark predicate
   * @param predicate Spark predicate
   * @param expectedPredicateNames names of the expected pushed Spark predicates
   * @param modelPredicate Scala-equivalent predicate of given Spark predicate
   */

  private def assertEffectOfPredicatePushdown(
                                               predicate: Column,
                                               expectedPredicateNames: Seq[String],
                                               modelPredicate: PushdownBean => Boolean
                                             ): Unit = {

    // Read data using dataSource
    val df = spark.read.format(Constants.DATASOURCE_NAME)
      .options(optionsForAuthAndIndex(pushdownPredicateIndex))
      .load().filter(predicate)

    // Retrieve pushed predicates
    val maybePushedPredicates = df.queryExecution.executedPlan.collect {
      case scan: BatchScanExec =>
        scan.scan
    }.headOption.collect {
      case scan: SearchScan => scan.pushedPredicates
    }

    // Assert that some predicates have been pushed down
    maybePushedPredicates shouldBe defined
    val predicates = maybePushedPredicates.get
    predicates should not be empty
    predicates.map(_.name()) should contain allElementsOf expectedPredicateNames

    // Assert that retrieved documents match
    val expectedDocuments: Seq[PushdownBean] = pushdownBeans.filter(modelPredicate)
    val ids: Seq[String] = df.collect().map(_.getAs[String]("id"))
    ids should have size expectedDocuments.size
    ids should contain theSameElementsAs expectedDocuments.map(_.id)
  }

  describe("Search dataSource") {
    describe(SHOULD) {
      describe("read documents") {
        it("that match a filter") {

          dropIndexIfExists(atomicBeansIndex, sleep = true)
          createIndexFromSchemaOf[AtomicBean](atomicBeansIndex)
          writeDocuments[AtomicBean](atomicBeansIndex, atomicBeans)
          val df = readUsingDatasource(atomicBeansIndex, Some("stringValue ne null"), None, None)
          df.count() shouldBe atomicBeans.count(_.stringValue.isDefined)
        }

        it("selecting some fields") {

          val select = Seq("id", "stringValue")
          val df = readUsingDatasource(atomicBeansIndex, None, Some(select), None)
          df.count() shouldBe atomicBeans.size
          df.columns should contain theSameElementsAs select
        }

        describe("translating") {
          describe("numeric values as") {
            it("strings") {

              // Create a schema with all string fields
              val schemaRead = schemaOfCaseClass[AtomicBean].map {
                sf => if (!sf.name.equals("id")) {
                  sf.copy(dataType = DataTypes.StringType)
                } else sf
              }

              // Read data and do assertions
              val rows: Seq[Row] = readUsingDatasource(atomicBeansIndex, None, None, Some(createStructType(schemaRead: _*))).collect()
              rows should have size atomicBeans.size
              forAll(zipRowsAndBeans(rows, atomicBeans)) {
                case (row, sample) =>
                  asserEffectOfEncodingOn[String](row, sample, "intValue", _.intValue.map(String.valueOf))
                  asserEffectOfEncodingOn[String](row, sample, "longValue", _.longValue.map(String.valueOf))
                  asserEffectOfEncodingOn[String](row, sample, "doubleValue", _.doubleValue.map(String.valueOf))
              }
            }

            it("numeric values of different type") {

              val schemaRead = createStructType(
                createStructField("id", DataTypes.StringType),
                createStructField("intValue", DataTypes.DoubleType),
                createStructField("longValue", DataTypes.DoubleType)
              )

              val rows = readUsingDatasource(atomicBeansIndex, None, None, Some(schemaRead)).collect()
              rows should have size atomicBeans.size
              forAll(zipRowsAndBeans(rows, atomicBeans)) {
                case (row, bean) =>
                  asserEffectOfEncodingOn[Double](row, bean, "intValue", _.intValue.map(_.doubleValue()))
                  asserEffectOfEncodingOn[Double](row, bean, "longValue", _.longValue.map(_.doubleValue()))
              }
            }
          }

          describe("boolean values as") {
            it("booleans") {

              val schemaRead = createStructType(
                createStructField("id", DataTypes.StringType),
                createStructField("booleanValue", DataTypes.BooleanType)
              )

              val rows = readUsingDatasource(atomicBeansIndex, None, None, Some(schemaRead)).collect()
              rows should have size atomicBeans.size
              forAll(zipRowsAndBeans(rows, atomicBeans)) {
                case (r, s) =>
                  asserEffectOfEncodingOn[Boolean](r, s, "booleanValue", _.booleanValue)
              }
            }

            it("strings") {

              val schemaRead = createStructType(
                createStructField("id", DataTypes.StringType),
                createStructField("booleanValue", DataTypes.StringType)
              )

              val rows = readUsingDatasource(atomicBeansIndex, None, None, Some(schemaRead)).collect()
              rows should have size atomicBeans.size
              forAll(zipRowsAndBeans(rows, atomicBeans)) {
                case (r, s) =>
                  asserEffectOfEncodingOn[String](r, s, "booleanValue", _.booleanValue.map(String.valueOf))
              }
            }
          }

          describe("datetimeoffset values as") {
            it("dates") {

              val schemaRead = createStructType(
                createStructField("id", DataTypes.StringType),
                createStructField("timestampValue", DataTypes.DateType)
              )

              val rows = readUsingDatasource(atomicBeansIndex, None, None, Some(schemaRead)).collect()
              rows should have size atomicBeans.size
              forAll(zipRowsAndBeans(rows, atomicBeans)) {
                case (r, s) =>
                  asserEffectOfEncodingOn[Date](r, s, "timestampValue", _.timestampValue.map {
                    t => Date.valueOf(t.toInstant.atOffset(Constants.UTC_OFFSET).format(DateTimeFormatter.ISO_LOCAL_DATE))
                  })
              }
            }

            it("timestamps") {

              val schemaRead = createStructType(
                createStructField("id", DataTypes.StringType),
                createStructField("timestampValue", DataTypes.TimestampType)
              )

              val rows = readUsingDatasource(atomicBeansIndex, None, None, Some(schemaRead)).collect()
              rows should have size atomicBeans.size
              forAll(zipRowsAndBeans(rows, atomicBeans)) {
                case (r, s) =>
                  asserEffectOfEncodingOn[Timestamp](r, s, "timestampValue", _.timestampValue)
              }
            }

            it("strings") {

              val schemaRead = createStructType(
                createStructField("id", DataTypes.StringType),
                createStructField("timestampValue", DataTypes.StringType)
              )

              val rows = readUsingDatasource(atomicBeansIndex, None, None, Some(schemaRead)).collect()
              rows should have size atomicBeans.size
              forAll(zipRowsAndBeans(rows, atomicBeans)) {
                case (r, s) =>
                  asserEffectOfEncodingOn[String](r, s, "timestampValue", _.timestampValue.map {
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

              dropIndexIfExists(atomicBeansIndex, sleep = false)
              dropIndexIfExists(collectionBeansIndex, sleep = true)
              val samples = Seq(
                CollectionBean[String]("hello", Some(Seq("world", "John"))),
                CollectionBean[String]("world", None)
              )

              createIndexFromSchemaOf[CollectionBean[String]](collectionBeansIndex)
              writeDocuments[CollectionBean[String]](collectionBeansIndex, samples)(
                CollectionBean.serializerFor[String]
              )
              val rows = readUsingDatasource(collectionBeansIndex, None, None, None).collect()
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
              val rows = toSeqOf[CollectionBean[ActionTypeBean]](readUsingDatasource(collectionBeansIndex, None, None, None))
              rows should have size samples.size
              forAll(rows.sortBy(_.id).zip(samples.sortBy(_.id))) {
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
                readUsingDatasource(collectionBeansIndex, None, None, None)
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

        describe("pushing down some predicates, like") {

          it("null equality") {

            dropIndexIfExists(collectionBeansIndex, sleep = true)
            createIndexFromSchemaOf[PushdownBean](pushdownPredicateIndex)
            writeDocuments[PushdownBean](pushdownPredicateIndex, pushdownBeans)

            // Evaluate pushdown for IS_NULL and IS_NOT_NULL
            assertEffectOfPredicatePushdown(col("stringValue").isNull, Seq("IS_NULL"), _.stringValue.isEmpty)
            assertEffectOfPredicatePushdown(col("stringValue").isNotNull, Seq("IS_NOT_NULL"), _.stringValue.isDefined)
          }

          it("comparisons") {

            // Equality
            val equalToOne: Int => Boolean = _.equals(1)
            assertEffectOfPredicatePushdown(col("intValue") === 1, Seq("="), _.intValue.exists(equalToOne))
            assertEffectOfPredicatePushdown(col("intValue") =!= 1, Seq("NOT"), _.intValue.exists(i => !equalToOne(i)))

            // Greater
            assertEffectOfPredicatePushdown(col("intValue") > 2, Seq(">"), _.intValue.exists(_ > 2))
            assertEffectOfPredicatePushdown(col("intValue") >= 2, Seq(">="), _.intValue.exists(_ >= 2))

            // Less
            assertEffectOfPredicatePushdown(col("intValue") < 2, Seq("<"), _.intValue.exists(_ < 2))
            assertEffectOfPredicatePushdown(col("intValue") <= 2, Seq("<="), _.intValue.exists(_ <= 2))
          }

          it("SQL-like IN expressions") {

            val inValues: Seq[String] = Seq("hello", "world")
            assertEffectOfPredicatePushdown(col("stringValue").isin(inValues: _*), Seq("IN"), _.stringValue.exists(inValues.contains))
          }

          it("logical combination of other predicates") {

            // And
            val stringValueNotNull: Column = col("stringValue").isNotNull
            val intValueNotNull: Column = col("intValue").isNotNull
            assertEffectOfPredicatePushdown(stringValueNotNull && intValueNotNull, Seq("IS_NOT_NULL"), p => p.stringValue.isDefined && p.intValue.isDefined)

            // Or
            assertEffectOfPredicatePushdown(stringValueNotNull || intValueNotNull, Seq("OR"), p => p.stringValue.isDefined || p.intValue.isDefined)
          }
        }
      }
    }
  }
}
