package io.github.jarol.azure.search.spark.sql.connector

import io.github.jarol.azure.search.spark.sql.connector.core.{IndexDoesNotExistException, JavaScalaConverters}
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
      it(s"throw a ${nameOf[IndexDoesNotExistException]} when inferring the schema of a non-existing index") {

        val indexName = "table-provider-spec"
        indexExists(indexName) shouldBe false
        an[IndexDoesNotExistException] shouldBe thrownBy {

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
