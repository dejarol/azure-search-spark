package com.github.jarol.azure.search.spark.sql.connector

import com.azure.search.documents.models.IndexActionType
import com.github.jarol.azure.search.spark.sql.connector.core.{Constants, JavaScalaConverters}
import com.github.jarol.azure.search.spark.sql.connector.models.{AbstractITDocument, ActionTypeBean, BaseActionTypeBean, SimpleBean}
import com.github.jarol.azure.search.spark.sql.connector.write.WriteConfig
import org.apache.spark.sql.SaveMode
import org.scalatest.Inspectors

import java.time.LocalDate
import scala.reflect.runtime.universe.TypeTag

class WriteSpec
  extends SearchSparkIntegrationSpec
    with Inspectors {

  private lazy val simpleBeansIndex = "write-simple-beans"
  private lazy val actionTypeBeans = "write-action-type-beans"

  override protected lazy val itSearchIndexNames: Seq[String] = Seq(
    simpleBeansIndex,
    actionTypeBeans
  )

  /**
   * Write a dataFrame to a Search index
   * @param documents data to write
   * @param indexName index name
   * @param extraOptions additional write options
   */

  private def writeToIndex[T <: AbstractITDocument with Product: TypeTag](
                                                                           indexName: String,
                                                                           documents: Seq[T],
                                                                           extraOptions: Option[Map[String, String]]
                                                                         ): Unit = {

    // Set up the writer
    val basicWriter = toDF(documents).write.format(Constants.DATASOURCE_NAME)
      .options(optionsForAuthAndIndex(indexName))
      .option(WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.KEY_FIELD, "id")

    // Add extra options, if needed
    extraOptions
      .map(basicWriter.options)
      .getOrElse(basicWriter)
      .mode(SaveMode.Append)
      .save()
  }

  /**
   * Read documents from an index as collection of instances of a target type
   * @param index index name
   * @tparam T target type (should have an implicit [[DocumentDeserializer]] in scope)
   * @return a collection of typed documents
   */

  private def readDocumentsAs[T: DocumentDeserializer](index: String): Seq[T] = {

    val deserializer = implicitly[DocumentDeserializer[T]]
    JavaScalaConverters.listToSeq(
      SearchTestUtils.readDocuments(getSearchClient(index))
    ).map {
      deserializer.deserialize(_)
    }
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
          val actual: Seq[SimpleBean] = readDocumentsAs[SimpleBean](simpleBeansIndex)
          actual should have size input.size
          forAll(actual.sortBy(_.id).zip(
            input.sortBy(_.id)
          )) {
            case (actual, expected) =>
              actual.id shouldBe expected.id
              actual.date shouldBe expected.date
              actual.insertTime shouldBe expected.insertTime
          }

          dropIndexIfExists(simpleBeansIndex, sleep = false)
        }

        it("not including the column used for index action type") {

          val indexActionColumn = "action"
          val documents: Seq[ActionTypeBean] = Seq(
            ActionTypeBean("hello", Some(1), IndexActionType.UPLOAD)
          )

          dropIndexIfExists(actionTypeBeans, sleep = true)
          indexExists(actionTypeBeans) shouldBe false

          val extraOptions = Map(
            WriteConfig.INDEX_ACTION_COLUMN_CONFIG -> indexActionColumn
          )

          writeToIndex(actionTypeBeans, documents, Some(extraOptions))
          indexExists(actionTypeBeans) shouldBe true
          val expectedSearchFieldNames = schemaOfCaseClass[ActionTypeBean].fields.collect {
            case sf if !sf.name.equalsIgnoreCase(indexActionColumn) => sf.name
          }

          val actualFieldNames = getIndexFields(actionTypeBeans).map(_.getName)
          actualFieldNames should contain theSameElementsAs expectedSearchFieldNames
          val actual: Seq[BaseActionTypeBean] = readDocumentsAs[BaseActionTypeBean](actionTypeBeans)
          actual should have size documents.size
          forAll(actual.sortBy(_.id).zip(
            documents.sortBy(_.id)
          )) {
            case (actual, expected) =>
              actual.id shouldBe expected.id
              actual.value shouldBe expected.value
          }

          dropIndexIfExists(actionTypeBeans, sleep = false)
        }
      }
    }
  }
}
