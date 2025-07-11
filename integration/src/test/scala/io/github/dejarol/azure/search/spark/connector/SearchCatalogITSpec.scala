package io.github.dejarol.azure.search.spark.connector

import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters
import io.github.dejarol.azure.search.spark.connector.core.config.IOConfig
import io.github.dejarol.azure.search.spark.connector.models.{AtomicBean, PushdownBean, SimpleBean}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class SearchCatalogITSpec
  extends SearchITSpec {

  import SearchCatalogITSpec._

  private lazy val catalog = initializeCatalog(propertiesSupplier)
  private lazy val (firstIndex, secondIndex) = ("simple-beans", "pushdown-beans")

  describe(anInstanceOf[SearchCatalog]) {
    describe(SHOULD) {
      it("list all tables within the catalog") {

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

      it("drop a table from the catalog") {

        val identifier = SearchCatalog.identifierOf(firstIndex)

        // The table does not exist, so we expected both assertions to return false
        indexExists(firstIndex) shouldBe false
        catalog.dropTable(identifier) shouldBe false

        // The table should exist now, so dropping the table should return true
        createIndexFromSchemaOf[SimpleBean](firstIndex)
        indexExists(firstIndex) shouldBe true
        catalog.dropTable(identifier) shouldBe true
        indexExists(firstIndex) shouldBe false
      }

      describe("load a table") {
        it(s"throwing a ${nameOf[NoSuchTableException]} for a non-existent table") {

          indexExists(firstIndex) shouldBe false
          a[NoSuchTableException] shouldBe thrownBy {
            catalog.loadTable(SearchCatalog.identifierOf(firstIndex))
          }
        }

        it("returning metadata for an existing table") {

          indexExists(firstIndex) shouldBe false
          createIndexFromSchemaOf[AtomicBean](firstIndex)
          indexExists(firstIndex) shouldBe true

          // Assertions on table metadata
          val table = catalog.loadTable(SearchCatalog.identifierOf(firstIndex))
          table.name() shouldBe firstIndex
          table.schema() should contain theSameElementsAs Encoders.product[AtomicBean].schema

          dropIndexIfExists(firstIndex, sleep = false)
        }
      }

      describe(SHOULD_NOT) {
        describe("allow some catalog operations, like") {
          it("renaming a table") {

            // Old table should not exist, so we should throw a NoSuchTableException
            a [NoSuchTableException] shouldBe thrownBy {
              catalog.renameTable(
                SearchCatalog.identifierOf("old"),
                SearchCatalog.identifierOf("new")
              )
            }

            // New table should not exist, so we should throw a TableAlreadyExistsException
            createIndexFromSchemaOf[AtomicBean]("old")
            a [TableAlreadyExistsException] shouldBe thrownBy {
              catalog.renameTable(
                SearchCatalog.identifierOf("old"),
                SearchCatalog.identifierOf("old")
              )
            }

            // Default case: operation is not supported
            an[UnsupportedOperationException] shouldBe thrownBy {
              catalog.renameTable(
                SearchCatalog.identifierOf("old"),
                SearchCatalog.identifierOf("new")
              )
            }

            dropIndexIfExists("old", sleep = false)
          }

          it("altering a table") {

            // If the table doesn't exist, we should throw a NoSuchTableException
            a[NoSuchTableException] shouldBe thrownBy {
              catalog.alterTable(
                SearchCatalog.identifierOf("old")
              )
            }

            // If the table exists, we should throw an UnsupportedOperationException
            createIndexFromSchemaOf[AtomicBean]("old")
            an[IllegalArgumentException] shouldBe thrownBy {
              catalog.alterTable(
                SearchCatalog.identifierOf("old")
              )
            }
          }
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

  private def initializeCatalog(propertiesSupplier: IntegrationPropertiesSupplier): SearchCatalog = {

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