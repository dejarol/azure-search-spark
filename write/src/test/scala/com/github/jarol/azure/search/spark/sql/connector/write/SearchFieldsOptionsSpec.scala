package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.core.schema.SearchFieldFeature
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, FieldFactory}
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

  /**
   * Create an instance of [[SearchFieldsOptions]] by configuring a non-empty list of field names
   * for a given feature, and an empty list for all other features
   * @param feature feature
   * @param key name of key field
   * @param list list of field names for which given feature should be enabled
   * @param indexActionColumn index action column
   * @return an instance of [[SearchFieldsOptions]]
   */

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

  /**
   * Assert that a feature has been enabled
   * @param featureAssertion feature assertion
   * @param key name of key field
   * @param list list of candidate enabled fields
   */

  private def assertFeatureEnablingFor(
                                        featureAssertion: FeatureEnabledAssertion,
                                        key: String,
                                        list: Seq[String]
                                      ): Unit = {

    val feature: SearchFieldFeature = featureAssertion.feature
    val options = createOptionsForFeature(feature, key, list, None)
    val (matchingFields, nonMatchingFields) = schema.partition(featureAssertion.shouldBeEnabled(options, _))
    val searchFields = getSearchFieldsMap(options, schema)
    forAll(nonMatchingFields) {
      sf =>
        feature.isDisabledOnField(searchFields(sf.name)) shouldBe true
    }

    forAll(matchingFields) {
      sf =>
        feature.isEnabledOnField(searchFields(sf.name)) shouldBe true
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
            FeatureEnabledAssertion.Facetable,
            "key",
            Seq(second)
          )
        }

        it("filterable") {

          assertFeatureEnablingFor(
            FeatureEnabledAssertion.Filterable,
            "key",
            Seq(first)
          )
        }

        it("hidden") {

          assertFeatureEnablingFor(
            FeatureEnabledAssertion.Hidden,
            "key",
            Seq(third)
          )
        }

        it("key") {

          assertFeatureEnablingFor(
            FeatureEnabledAssertion.Key,
            second,
            Seq.empty
          )
        }

        it("searchable") {

          assertFeatureEnablingFor(
            FeatureEnabledAssertion.Searchable,
            "key",
            Seq(second)
          )
        }

        it("sortable") {

          assertFeatureEnablingFor(
            FeatureEnabledAssertion.Sortable,
            "key",
            Seq(second)
          )
        }
      }
    }
  }
}
