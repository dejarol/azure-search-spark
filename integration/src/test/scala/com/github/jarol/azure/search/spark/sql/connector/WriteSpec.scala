package com.github.jarol.azure.search.spark.sql.connector

import com.azure.search.documents.models.IndexActionType
import com.github.jarol.azure.search.spark.sql.connector.core.schema.SearchFieldFeature
import com.github.jarol.azure.search.spark.sql.connector.core.{Constants, JavaScalaConverters}
import com.github.jarol.azure.search.spark.sql.connector.models.{AbstractITDocument, ActionTypeBean, BaseActionTypeBean, FeaturesBean, SimpleBean}
import com.github.jarol.azure.search.spark.sql.connector.write.WriteConfig
import org.apache.spark.sql.SaveMode
import org.scalatest.Inspectors

import java.time.LocalDate
import scala.reflect.runtime.universe.TypeTag

class WriteSpec
  extends SearchSparkIntegrationSpec
    with Inspectors {

  private lazy val simpleBeansIndex = "write-simple-beans"
  private lazy val actionTypeIndex = "write-action-type-beans"
  private lazy val featuresIndex = "write-features-index"

  override protected lazy val itSearchIndexNames: Seq[String] = Seq(
    simpleBeansIndex,
    actionTypeIndex,
    featuresIndex
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

  private def assertFeatureEnablingOnIndex(
                                            indexName: String,
                                            names: Seq[String],
                                            feature: SearchFieldFeature
                                          ): Unit = {

    val (expectedNotEnabled, expectedEnabled) = getIndexFields(indexName)
      .partition {
        p => names.exists {
          _.equalsIgnoreCase(p.getName)
      }
    }

    forAll(expectedNotEnabled) {
      field => feature.isDisabledOnField(field) shouldBe true
    }

    forAll(expectedEnabled) {
      field => feature.isEnabledOnField(field) shouldBe true
    }
  }

  describe("Search dataSource") {
    describe(SHOULD) {
      describe("create an index (if it does not exist)") {
        ignore("with as many fields as many columns") {

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

        ignore("not including the column used for index action type") {

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
          val actual: Seq[BaseActionTypeBean] = readDocumentsAs[BaseActionTypeBean](actionTypeIndex)
          actual should have size documents.size
          forAll(actual.sortBy(_.id).zip(
            documents.sortBy(_.id)
          )) {
            case (actual, expected) =>
              actual.id shouldBe expected.id
              actual.value shouldBe expected.value
          }

          dropIndexIfExists(actionTypeIndex, sleep = false)
        }

        describe("allowing the user to enable field properties, like being") {
          it("facetable") {

            val documents = Seq(
              FeaturesBean("hello", Some("DISCOUNT"), Some(0))
            )

            val facetableFields = Seq("category")
            val extraOptions = Map(
              WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.FACETABLE_FIELDS -> facetableFields.mkString(",")
            )

            dropIndexIfExists(featuresIndex, sleep = true)
            writeToIndex(featuresIndex, documents, Some(extraOptions))
            assertFeatureEnablingOnIndex(featuresIndex, facetableFields, SearchFieldFeature.FACETABLE)
          }

          it("filterable") {

            // TODO: test
          }

          it("hidden") {

            // TODO: test
          }

          it("searchable") {

            // TODO: test
          }

          it("sortable") {

          }
        }
      }
    }
  }
}
