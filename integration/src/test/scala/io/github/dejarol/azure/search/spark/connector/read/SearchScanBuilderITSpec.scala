package io.github.dejarol.azure.search.spark.connector.read

import io.github.dejarol.azure.search.spark.connector.SearchITSpec
import io.github.dejarol.azure.search.spark.connector.core.IndexDoesNotExistException
import io.github.dejarol.azure.search.spark.connector.read.config.ReadConfig
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
