package com.github.jarol.azure.search.spark.sql.connector

import com.github.jarol.azure.search.spark.sql.connector.core.{Constants, FieldFactory, IndexDoesNotExistException}
import com.github.jarol.azure.search.spark.sql.connector.models.{AtomicBean, SimpleBean}
import com.github.jarol.azure.search.spark.sql.connector.write.WriteConfig
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.scalatest.Inspectors

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}

class ReadSpec
  extends SearchSparkSpec
    with RowMixins
      with FieldFactory
        with Inspectors {

  /**
   * Zip together rows and numeric beans, sorting both by id
   * @param rows rows
   * @param beans beans
   * @return a collection of tuples
   */

  private def zipRowsAndBeans(
                               rows: Seq[Row],
                               beans: Seq[AtomicBean]
                             ): Seq[(Row, AtomicBean)] = {

    rows
      .sortBy(_.getAs[String]("id"))
      .zip(beans.sortBy(_.id))
  }

  /**
   * Assert that a row value has been encoded correctly w.r.t. its original value
   * @param output output row (retrieved from a Search index)
   * @param input a DataFrame row written to the Search index
   * @param colName column name
   * @param encodingFunction expected encoding function from the input to the output
   * @tparam T encoding target type
   */

  private def assertEncoding[T](
                                 output: Row,
                                 input: AtomicBean,
                                 colName: String,
                                 encodingFunction: AtomicBean => Option[T]
                               ): Unit = {

    output.getAsOpt[T](colName) shouldBe encodingFunction(input)
  }

  describe("Search dataSource") {
    describe(SHOULD) {
      describe("throw an exception when") {
        it("target index does not exist") {

          val indexName = "non-existing"
          dropIndexIfExists(indexName)
          indexExists(indexName) shouldBe false
          a [IndexDoesNotExistException] shouldBe thrownBy {
            readIndex(indexName, None, None, None)
          }

          dropIndexIfExists(indexName)
        }
      }

      describe("read documents") {
        it("that match a filter") {

          val (indexName, id) = ("simple-beans", "hello")
          val input = Seq(
            SimpleBean(id, Some(LocalDate.now())),
            SimpleBean("world", Some(LocalDate.now().plusDays(1)))
          )

          dropIndexIfExists(indexName)
          indexExists(indexName) shouldBe false

          val writeExtraOptions = Map(
            WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.FILTERABLE_FIELDS -> "id"
          )

          writeToIndex(toDF(input), indexName, "id", Some(writeExtraOptions))

          // Wait some time to ensure result consistency
          Thread.sleep(5000)
          indexExists(indexName) shouldBe true
          val df = readIndex(indexName, Some(s"id eq '$id'"), None, Some(schemaOfCaseClass[SimpleBean]))

          val output = toSeqOf[SimpleBean](df)
          output should have size 1
          output.head.id shouldBe id

          dropIndexIfExists(indexName)
          indexExists(indexName) shouldBe false
        }

        it("selecting some fields") {

          val indexName = "select-beans"
          dropIndexIfExists(indexName)
          indexExists(indexName) shouldBe false

          val input = Seq(
            SimpleBean("hello", Some(LocalDate.now())),
            SimpleBean("world", Some(LocalDate.now().plusDays(1)))
          )

          dropIndexIfExists(indexName)
          indexExists(indexName) shouldBe false
          writeToIndex(toDF(input), indexName, "id", None)

          // Wait some time to ensure result consistency
          Thread.sleep(5000)

          val select = Seq("id", "date")
          val df = readIndex(indexName, None, Some(select), None)
          df.count() shouldBe input.size
          df.columns should contain theSameElementsAs select

          dropIndexIfExists(indexName)
          indexExists(indexName) shouldBe false
        }

        describe("translating") {

          // Write some data
          val indexName = "numeric-beans"
          val notNullBean = AtomicBean("notNullId", Some(1), Some(123), Some(3.45), Some(4.56f), Some(false), Some(LocalDateTime.now()))
          val nullBean = AtomicBean("nullId", None, None, None, None, None, None.asInstanceOf[Option[LocalDateTime]])
          val samples = Seq(notNullBean, nullBean)

          dropIndexIfExists(indexName)
          writeToIndex(toDF(samples), indexName, "id", None)
          Thread.sleep(5000)

          describe("numeric values as") {
            it("strings") {

              // Create a schema with all string fields
              val schemaRead = schemaOfCaseClass[AtomicBean].map {
                sf => if (!sf.name.equals("id")) sf.copy(dataType = DataTypes.StringType) else sf
              }

              // Read data and do assertions
              val rows: Seq[Row] = readIndex(indexName, None, None, Some(StructType(schemaRead))).collect()
              rows should have size samples.size
              forAll(
                zipRowsAndBeans(rows, samples)
              ) {
                case (row, sample) =>
                  assertEncoding[String](row, sample, "intValue", _.intValue.map(String.valueOf))
                  assertEncoding[String](row, sample, "intValue", _.intValue.map(String.valueOf))
                  assertEncoding[String](row, sample, "longValue", _.longValue.map(String.valueOf))
                  assertEncoding[String](row, sample, "doubleValue", _.doubleValue.map(String.valueOf))
                  assertEncoding[String](row, sample, "floatValue", _.floatValue.map(String.valueOf))
              }
            }

            it("numeric values of different type") {

              val schemaRead = createStructType(
                createStructField("intValue", DataTypes.DoubleType),
                createStructField("longValue", DataTypes.FloatType)
              )

              val rows = readIndex(indexName, None, None, Some(schemaRead)).collect()
              rows should have size samples.size
              forAll(
                zipRowsAndBeans(rows, samples)
              ) {
                case (row, bean) =>
                  assertEncoding[Double](row, bean, "intValue", _.intValue.map(_.doubleValue()))
                  assertEncoding[Float](row, bean, "longValue", _.longValue.map(_.floatValue()))
              }
            }
          }

          describe("boolean values as") {
            it("booleans") {

              val schemaRead = createStructType(
                createStructField("booleanValue", DataTypes.BooleanType)
              )

              val rows = readIndex(indexName, None, None, Some(schemaRead)).collect()
              rows should have size samples.size
              forAll(
                zipRowsAndBeans(rows, samples)
              ) {
                case (r, s) =>
                  assertEncoding[Boolean](r, s, "booleanBalue", _.booleanValue)
              }
            }

            it("strings") {

              val schemaRead = createStructType(
                createStructField("booleanValue", DataTypes.StringType)
              )

              val rows = readIndex(indexName, None, None, Some(schemaRead)).collect()
              rows should have size samples.size
              forAll(
                zipRowsAndBeans(rows, samples)
              ) {
                case (r, s) =>
                  assertEncoding[String](r, s, "booleanBalue", _.booleanValue.map(String.valueOf))
              }
            }
          }

          describe("datetimeoffset values as") {
            it("dates") {

              val schemaRead = createStructType(
                createStructField("timestampValue", DataTypes.DateType)
              )

              val rows = readIndex(indexName, None, None, Some(schemaRead)).collect()
              rows should have size samples.size
              forAll(
                zipRowsAndBeans(rows, samples)
              ) {
                case (r, s) =>
                  assertEncoding[Date](r, s, "timestampValue", _.timestampValue.map {
                    t => Date.valueOf(t.toLocalDateTime.toLocalDate)
                  })
              }
            }

            it("timestamps") {

              val schemaRead = createStructType(
                createStructField("timestampValue", DataTypes.TimestampType)
              )

              val rows = readIndex(indexName, None, None, Some(schemaRead)).collect()
              rows should have size samples.size
              forAll(
                zipRowsAndBeans(rows, samples)
              ) {
                case (r, s) =>
                  assertEncoding[Timestamp](r, s, "timestampValue", _.timestampValue)
              }
            }

            it("strings") {

              val schemaRead = createStructType(
                createStructField("timestampValue", DataTypes.StringType)
              )

              val rows = readIndex(indexName, None, None, Some(schemaRead)).collect()
              rows should have size samples.size
              forAll(
                zipRowsAndBeans(rows, samples)
              ) {
                case (r, s) =>
                  assertEncoding[String](r, s, "timestampValue", _.timestampValue.map {
                    _.toLocalDateTime.format(Constants.DATETIME_OFFSET_FORMATTER)
                  })
              }
            }
          }

          dropIndexIfExists(indexName)
        }
      }
    }
  }
}
