package com.github.jarol.azure.search.spark.sql.connector

import com.azure.search.documents.models.IndexActionType
import com.github.jarol.azure.search.spark.sql.connector.models.{ActionTypeBean, SimpleBean}
import com.github.jarol.azure.search.spark.sql.connector.write.WriteConfig
import org.apache.spark.sql.SaveMode

import java.time.LocalDate

class WriteSpec
  extends SearchSparkSpec {

  describe(s"Datasource '${SearchTableProvider.SHORT_NAME}'") {
    describe(SHOULD) {
      describe("create an index if it does not exist") {
        it("with as many fields as many dataframe columns") {

          val indexName = "simple-beans"
          val documents: Seq[SimpleBean] = Seq(
            SimpleBean("hello", Some(LocalDate.now()))
          )

          dropIndexIfExists(indexName)
          indexExists(indexName) shouldBe false
          toDF(documents).write.format(SearchTableProvider.SHORT_NAME)
            .options(optionsForAuthAndIndex(indexName))
            .option(WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.KEY_FIELD, "id")
            .mode(SaveMode.Append)
            .save()

          indexExists(indexName) shouldBe true
          val expectedSearchFieldNames = schemaOfCaseClass[SimpleBean].fields.map(_.name)
          val actualFieldNames = getIndexFields(indexName).map(_.getName)
          actualFieldNames should contain theSameElementsAs expectedSearchFieldNames
          dropIndex(indexName)
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
          toDF(documents).write.format(SearchTableProvider.SHORT_NAME)
            .options(optionsForAuthAndIndex(indexName))
            .option(WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.KEY_FIELD, "id")
            .option(WriteConfig.INDEX_ACTION_COLUMN_CONFIG, indexActionColumn)
            .mode(SaveMode.Append)
            .save()

          indexExists(indexName) shouldBe true
          val expectedSearchFieldNames = schemaOfCaseClass[ActionTypeBean].fields.collect {
            case sf if !sf.name.equalsIgnoreCase(indexActionColumn) => sf.name
          }

          val actualFieldNames = getIndexFields(indexName).map(_.getName)
          actualFieldNames should contain theSameElementsAs expectedSearchFieldNames
          dropIndex(indexName)
          indexExists(indexName) shouldBe false
        }
      }
    }
  }
}
