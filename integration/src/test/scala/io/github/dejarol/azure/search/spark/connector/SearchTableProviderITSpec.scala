package io.github.dejarol.azure.search.spark.connector

import io.github.dejarol.azure.search.spark.connector.core.{NoSuchSearchIndexException, JavaScalaConverters}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class SearchTableProviderITSpec
  extends SearchITSpec {

  private lazy val tableProvider = new SearchTableProvider

  /**
   * Create a [[CaseInsensitiveStringMap]] from a simple Scala map
   * @param map Scala map
   * @return a case-insensitive map
   */

  private def createCIMap(map: Map[String, String]): CaseInsensitiveStringMap = {

    new CaseInsensitiveStringMap(
      JavaScalaConverters.scalaMapToJava(map)
    )
  }

  describe(anInstanceOf[SearchTableProvider]) {
    describe(SHOULD) {
      it(s"throw a ${nameOf[NoSuchSearchIndexException]} when inferring the schema of a non-existing index") {

        val indexName = "table-provider-spec"
        indexExists(indexName) shouldBe false
        an[NoSuchSearchIndexException] shouldBe thrownBy {

          tableProvider.inferSchema(
            createCIMap(
              optionsForAuthAndIndex(indexName)
            )
          )
        }
      }
    }
  }
}
