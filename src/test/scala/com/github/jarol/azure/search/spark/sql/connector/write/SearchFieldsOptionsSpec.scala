package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.schema.SearchFieldFeature
import com.github.jarol.azure.search.spark.sql.connector.{BasicSpec, FieldFactory}
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.scalatest.Inspectors

class SearchFieldsOptionsSpec
  extends BasicSpec
    with FieldFactory
      with Inspectors {

  private type OSS = Option[Seq[String]]
  private lazy val (first, second, third, fourth) = ("first", "second", "third", "fourth")
  private lazy val schema = Seq(
    createStructField(first, DataTypes.StringType),
    createStructField(second, DataTypes.IntegerType),
    createStructField(third, DataTypes.DateType)
  )

  private def createOptionsForFeature(feature: SearchFieldFeature, list: Seq[String]): SearchFieldsOptions = {

    val (facetable, filterable, hidden, searchable, sortable): (OSS, OSS, OSS, OSS, OSS) = feature match {
      case SearchFieldFeature.FACETABLE => (Some(list), None, None, None, None)
      case SearchFieldFeature.FILTERABLE => (None, Some(list), None, None, None)
      case SearchFieldFeature.HIDDEN => (None, None, Some(list), None, None)
      case SearchFieldFeature.KEY => (None, None, None, None, None)
      case SearchFieldFeature.SEARCHABLE => (None, None, None, Some(list), None)
      case SearchFieldFeature.SORTABLE => (None, None, None, None, Some(list))
    }

    SearchFieldsOptions(
      "key",
      filterableFields = filterable,
      sortableFields = sortable,
      hiddenFields = hidden,
      searchableFields = searchable,
      facetableFields = facetable,
      None
    )
  }

  /**
   * Get a map with all Search index fields
   * @param options options for index creation
   * @param schema input schema
   * @return a map with keys being field names and values being field themselves
   */

  private def getSearchFields(
                               options: SearchFieldsOptions,
                               schema: Seq[StructField]
                             ): Map[String, SearchField] = {

    options.schemaToSearchFields(schema)
      .map {
        sf => (sf.getName, sf)
      }.toMap
  }

  private def assertFeatureEnablingFor(
                         feature: SearchFieldFeature,
                         list: Seq[String],
                         predicate: (SearchFieldsOptions, StructField) => Boolean
                       ): Unit = {

    val options = createOptionsForFeature(feature, list)
    val inListPredicate: StructField => Boolean = sf => list.exists {
      _.equalsIgnoreCase(sf.name)
    }

    val fieldNotInList = schema.filterNot(inListPredicate)
    val fieldInList = schema.filter(inListPredicate)
    val searchFields = getSearchFields(options, schema)

    forAll(fieldNotInList) {
      sf =>
        predicate(options, sf) shouldBe false
        feature.isEnabled(searchFields(sf.name)) shouldBe false
    }

    forAll(fieldInList) {
      sf =>
        predicate(options, sf) shouldBe true
        feature.isEnabled(searchFields(sf.name)) shouldBe true
    }
  }

  describe(anInstanceOf[SearchFieldsOptions]) {
    describe(SHOULD) {
      it("not include index action column") {

        val indexActionColumn = fourth
        val input = schema :+ createStructField(indexActionColumn, DataTypes.StringType)
        val actual = SearchFieldsOptions(
            "key",
            None,
            None,
            None,
            None,
            None,
            Some(fourth)
          ).maybeExcludeIndexActionColumn(schema)

        actual should not be empty
        val expectedFieldNames = input.collect {
          case sf if !sf.name.equalsIgnoreCase(indexActionColumn) => sf.name
        }

        val actualFieldNames = actual.map(_.name)
        actualFieldNames should contain theSameElementsAs expectedFieldNames
      }

      describe("evaluate and eventually enable a feature for fields in a schema") {
        it("facetable") {

          assertFeatureEnablingFor(
            SearchFieldFeature.FACETABLE,
            Seq(second),
            _.isFacetable(_)
          )
        }

        it("filterable") {

          assertFeatureEnablingFor(
            SearchFieldFeature.FILTERABLE,
            Seq(first),
            _.isFilterable(_)
          )
        }

        it("hidden") {

          // TODO
        }
      }
    }
  }
}
