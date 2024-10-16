package com.github.jarol.azure.search.spark.sql.connector

import com.azure.search.documents.models.IndexActionType
import com.github.jarol.azure.search.spark.sql.connector.models.{ActionTypeBean, SimpleBean}
import com.github.jarol.azure.search.spark.sql.connector.write.WriteConfig

import java.time.LocalDate

class WriteSpec
  extends SearchSparkSpec {

  describe("Search dataSource") {
    describe(SHOULD) {
      describe("create an index (if it does not exist)") {
        it("with as many fields as many columns") {

          val indexName = "simple-beans"
          val documents: Seq[SimpleBean] = Seq(
            SimpleBean("hello", Some(LocalDate.now()))
          )

          dropIndexIfExists(indexName)
          indexExists(indexName) shouldBe false
          writeToIndex(toDF(documents), indexName, "id", None)

          indexExists(indexName) shouldBe true
          val expectedSearchFieldNames = schemaOfCaseClass[SimpleBean].fields.map(_.name)
          val actualFieldNames = getIndexFields(indexName).map(_.getName)
          actualFieldNames should contain theSameElementsAs expectedSearchFieldNames
          dropIndexIfExists(indexName)
          indexExists(indexName) shouldBe false
        }

        it("not including the column used for index action type") {

          val indexName = "action-type-beans"
          val indexActionColumn = "action"
          val documents: Seq[ActionTypeBean] = Seq(
            ActionTypeBean("hello", Some(1), IndexActionType.UPLOAD)
          )

          dropIndexIfExists(indexName)
          indexExists(indexName) shouldBe false

          val extraOptions = Map(
            WriteConfig.INDEX_ACTION_COLUMN_CONFIG -> indexActionColumn
          )

          writeToIndex(toDF(documents), indexName, "id", Some(extraOptions))
          indexExists(indexName) shouldBe true
          val expectedSearchFieldNames = schemaOfCaseClass[ActionTypeBean].fields.collect {
            case sf if !sf.name.equalsIgnoreCase(indexActionColumn) => sf.name
          }

          val actualFieldNames = getIndexFields(indexName).map(_.getName)
          actualFieldNames should contain theSameElementsAs expectedSearchFieldNames
          dropIndexIfExists(indexName)
          indexExists(indexName) shouldBe false
        }
      }
    }
  }
}
