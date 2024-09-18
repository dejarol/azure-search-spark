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

  private def createOptionsForFeature(
                                       feature: SearchFieldFeature,
                                       key: String,
                                       list: Seq[String],
                                       indexActionColumn: Option[String]
                                     ): SearchFieldsOptions = {

    val (facetable, filterable, hidden, searchable, sortable): (OSS, OSS, OSS, OSS, OSS) = feature match {
      case SearchFieldFeature.FACETABLE => (Some(list), None, None, None, None)
      case SearchFieldFeature.FILTERABLE => (None, Some(list), None, None, None)
      case SearchFieldFeature.HIDDEN => (None, None, Some(list), None, None)
      case SearchFieldFeature.KEY => (None, None, None, None, None)
      case SearchFieldFeature.SEARCHABLE => (None, None, None, Some(list), None)
      case SearchFieldFeature.SORTABLE => (None, None, None, None, Some(list))
    }

    SearchFieldsOptions(
      key,
      filterableFields = filterable,
      sortableFields = sortable,
      hiddenFields = hidden,
      searchableFields = searchable,
      facetableFields = facetable,
      indexActionColumn
    )
  }

  /**
   * Get a map with all Search index fields
   * @param options options for index creation
   * @param schema input schema
   * @return a map with keys being field names and values being field themselves
   */

  private def getSearchFieldsMap(
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
                                        key: String,
                                        list: Seq[String],
                                        predicate: (SearchFieldsOptions, StructField) => Boolean
                                      ): Unit = {

    val options = createOptionsForFeature(feature, key, list, None)
    val featurePredicate: StructField => Boolean = feature match {
      case SearchFieldFeature.KEY => _.name.equalsIgnoreCase(key)
      case _ => sf =>
        list.exists {
          _.equalsIgnoreCase(sf.name)
        }
    }

    val nonMatchingFields = schema.filterNot(featurePredicate)
    val matchingFields = schema.filter(featurePredicate)
    val searchFields = getSearchFieldsMap(options, schema)
    forAll(nonMatchingFields) {
      sf =>
        predicate(options, sf) shouldBe false
        feature.isEnabled(searchFields(sf.name)) shouldBe false
    }

    forAll(matchingFields) {
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
        val actual = createOptionsForFeature(SearchFieldFeature.SEARCHABLE, "key", Seq.empty, Some(fourth))
          .maybeExcludeIndexActionColumn(schema)

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
            "key",
            Seq(second),
            _.isFacetable(_)
          )
        }

        it("filterable") {

          assertFeatureEnablingFor(
            SearchFieldFeature.FILTERABLE,
            "key",
            Seq(first),
            _.isFilterable(_)
          )
        }

        it("hidden") {

          assertFeatureEnablingFor(
            SearchFieldFeature.HIDDEN,
            "key",
            Seq(third),
            _.isHidden(_)
          )
        }

        it("key") {

          assertFeatureEnablingFor(
            SearchFieldFeature.KEY,
            second,
            Seq.empty,
            _.isKey(_)
          )
        }

        it("searchable") {

          assertFeatureEnablingFor(
            SearchFieldFeature.SEARCHABLE,
            "key",
            Seq(second),
            _.isSearchable(_)
          )
        }

        it("sortable") {

          assertFeatureEnablingFor(
            SearchFieldFeature.SORTABLE,
            "key",
            Seq(second),
            _.isSortable(_)
          )
        }
      }
    }
  }
}
