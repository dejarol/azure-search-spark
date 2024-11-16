package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.SearchITSpec
import com.github.jarol.azure.search.spark.sql.connector.core.IndexDoesNotExistException
import org.apache.spark.sql.types.{DataTypes, StructType}

class SearchScanBuilderITSpec
  extends SearchITSpec {

  /**
   * Create a [[SearchScanBuilder]] instance
   * @param options read options
   * @param schema schema
   * @return a builder instance
   */

  private def createScanBuilder(
                                 options: Map[String, String],
                                 schema: StructType
                               ): SearchScanBuilder = {

    new SearchScanBuilder(
      ReadConfig(options),
      schema
    )
  }

  describe(anInstanceOf[SearchScanBuilder]) {
    describe(SHOULD) {
      it(s"throw a ${nameOf[IndexDoesNotExistException]} for non-existing index") {

        val index = "sca-builder-index"
        indexExists(index) shouldBe false
        val scanBuilder = createScanBuilder(
          optionsForAuthAndIndex(index),
          createStructType(
            createStructField("name", DataTypes.StringType)
          )
        )

        a[IndexDoesNotExistException] shouldBe thrownBy {
          scanBuilder.build()
        }
      }
    }
  }
}
