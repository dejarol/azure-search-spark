package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.SearchITSpec
import com.github.jarol.azure.search.spark.sql.connector.core.{FieldFactory, IndexDoesNotExistException}
import org.apache.spark.sql.types.DataTypes

class SearchScanBuilderITSpec
  extends SearchITSpec
    with FieldFactory {

  describe(anInstanceOf[SearchScanBuilder]) {
    describe(SHOULD) {
      it(s"throw a ${nameOf[IndexDoesNotExistException]} for non-existing index") {

        val index = "sca-builder-index"
        indexExists(index) shouldBe false
        val scanBuilder = new SearchScanBuilder(
          createStructType(
            createStructField("name", DataTypes.StringType)
          ),
          ReadConfig(
            optionsForAuthAndIndex(index)
          )
        )

        a[IndexDoesNotExistException] shouldBe thrownBy {
          scanBuilder.build()
        }
      }
    }
  }
}
