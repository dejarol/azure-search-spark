package com.github.jarol.azure.search.spark.sql.connector.schema

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.{BasicSpec, FieldFactory}
import org.apache.spark.sql.types.{DataTypes, StructField}

class SearchFieldOperationsSpec
  extends BasicSpec
    with FieldFactory {

  private lazy val (first, second) = ("first", "second")
  private lazy val searchField = createSearchField(first, SearchFieldDataType.STRING)

  describe(anInstanceOf[SearchFieldOperations]) {
    describe(SHOULD) {
      it(s"evaluate if the field has the same name with respect to a ${nameOf[StructField]}") {

        createSearchField(first, SearchFieldDataType.STRING)
          .sameNameOf(
            createStructField(first, DataTypes.IntegerType)
          ) shouldBe true
      }

      describe("enable some field properties, like") {
        it("key") {

          searchField.isKey shouldBe null
          searchField.maybeSetKey(second).isKey shouldBe null
          searchField.maybeSetKey(first).isKey shouldBe true
        }

        it("filterable") {

          searchField.isFilterable shouldBe null
          searchField.maybeSetFilterable(Some(Seq(second))).isFilterable shouldBe null
          searchField.maybeSetFilterable(Some(Seq(first))).isFilterable shouldBe true
        }

        it("sortable") {

          searchField.isSortable shouldBe null
          searchField.maybeSetSortable(Some(Seq(second))).isSortable shouldBe null
          searchField.maybeSetSortable(Some(Seq(first))).isSortable shouldBe true
        }

        it("hidden") {

          searchField.isHidden shouldBe null
          searchField.maybeSetHidden(Some(Seq(second))).isHidden shouldBe null
          searchField.maybeSetHidden(Some(Seq(first))).isHidden shouldBe true
        }

        it("facetable") {

          searchField.isFacetable shouldBe null
          searchField.maybeSetFacetable(Some(Seq(second))).isFacetable shouldBe null
          searchField.maybeSetFacetable(Some(Seq(first))).isFacetable shouldBe true
        }
      }
    }
  }
}
