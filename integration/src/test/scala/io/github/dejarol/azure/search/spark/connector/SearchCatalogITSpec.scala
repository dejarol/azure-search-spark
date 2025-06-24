package io.github.dejarol.azure.search.spark.connector

import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters
import io.github.dejarol.azure.search.spark.connector.core.config.IOConfig
import io.github.dejarol.azure.search.spark.connector.models.{PushdownBean, SimpleBean}
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class SearchCatalogITSpec
  extends SearchITSpec {

  import SearchCatalogITSpec._

  private lazy val catalog = initializeCatalog(propertiesSupplier)

  describe(anInstanceOf[SearchCatalog]) {
    describe(SHOULD) {
      it("list all tables within the catalog") {

        val (firstIndex, secondIndex) = ("simple-beans", "pushdown-beans")
        createIndexFromSchemaOf[SimpleBean](firstIndex)
        createIndexFromSchemaOf[PushdownBean](secondIndex)

        val tables = catalog.listTables(Array.empty)
        tables should have size 2
        forAll(tables.toSeq) {
          _.namespace() shouldBe empty
        }

        val tableNames = tables.map(_.name())
        tableNames should contain theSameElementsAs Seq(firstIndex, secondIndex)

        // Clean up
        dropIndexIfExists(firstIndex, sleep = false)
        dropIndexIfExists(secondIndex, sleep = false)
      }
    }

    describe(SHOULD_NOT) {
      describe("allow some catalog operations, like") {
        it("renaming a table") {

          // TODO: test
        }
      }
    }
  }
}

object SearchCatalogITSpec {

  /**
   * Create and initialize a catalog instance
   * @param propertiesSupplier supplier for getting integration properties
   * @return the [[org.apache.spark.sql.connector.catalog.TableCatalog]] implementation of this datasource
   */

  private def initializeCatalog(propertiesSupplier: IntegrationPropertiesSupplier): TableCatalog = {

    val catalog = new SearchCatalog
    val options = new CaseInsensitiveStringMap(
      JavaScalaConverters.scalaMapToJava(
        Map(
          IOConfig.END_POINT_CONFIG -> propertiesSupplier.endPoint(),
          IOConfig.API_KEY_CONFIG -> propertiesSupplier.apiKey()
        )
      )
    )

    // Initialize and return the catalog
    catalog.initialize("testCatalog", options)
    catalog
  }
}