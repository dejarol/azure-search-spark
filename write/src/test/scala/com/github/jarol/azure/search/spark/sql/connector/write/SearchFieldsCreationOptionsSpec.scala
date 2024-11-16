package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.core.schema.SearchFieldFeature
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, FieldFactory}
import org.apache.spark.sql.types.{DataTypes, StructField}

class SearchFieldsCreationOptionsSpec
  extends BasicSpec
    with FieldFactory {

  private type OSS = Option[Seq[String]]
  private lazy val (first, second, third, fourth) = ("first", "second", "third", "fourth")
  private lazy val schema = Seq(
    createStructField(first, DataTypes.StringType),
    createStructField(second, DataTypes.IntegerType),
    createStructField(third, DataTypes.DateType)
  )

  /**
   * Create an instance of [[SearchFieldsCreationOptions]] by configuring a non-empty list of field names
   * for a given feature, and an empty list for all other features
 *
   * @param feature feature
   * @param key name of key field
   * @param list list of field names for which given feature should be disabled
   * @param indexActionColumn index action column
   * @return an instance of [[SearchFieldsCreationOptions]]
   */

  private def createOptionsForFeature(
                                       feature: SearchFieldFeature,
                                       key: String,
                                       list: Seq[String],
                                       indexActionColumn: Option[String]
                                     ): SearchFieldsCreationOptions = {

    val (facetable, filterable, hidden, searchable, sortable): (OSS, OSS, OSS, OSS, OSS) = feature match {
      case SearchFieldFeature.FACETABLE => (Some(list), None, None, None, None)
      case SearchFieldFeature.FILTERABLE => (None, Some(list), None, None, None)
      case SearchFieldFeature.HIDDEN => (None, None, Some(list), None, None)
      case SearchFieldFeature.KEY => (None, None, None, None, None)
      case SearchFieldFeature.SEARCHABLE => (None, None, None, Some(list), None)
      case SearchFieldFeature.SORTABLE => (None, None, None, None, Some(list))
    }

    SearchFieldsCreationOptions(
      key,
      disabledFromFiltering = filterable,
      disabledFromSorting = sortable,
      hiddenFields = hidden,
      disabledFromSearch = searchable,
      disabledFromFaceting = facetable,
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
                                  options: SearchFieldsCreationOptions,
                                  schema: Seq[StructField]
                                ): Map[String, SearchField] = {

    options.schemaToSearchFields(schema)
      .map {
        sf => (sf.getName, sf)
      }.toMap
  }

  /**
   * Assert that a feature has been enabled/disabled
   * @param featureAssertion feature assertion
   * @param key name of key field
   * @param list list of candidate enabled fields
   */

  private def assertFeatureStatusesFor(
                                        featureAssertion: FeatureAssertion,
                                        key: String,
                                        list: Seq[String]
                                      ): Unit = {

    val feature: SearchFieldFeature = featureAssertion.feature
    val options = createOptionsForFeature(feature, key, list, None)
    val (matchingFields, nonMatchingFields) = schema.partition(featureAssertion.shouldBeAltered(_, options))
    val searchFields = getSearchFieldsMap(options, schema)
    forAll(matchingFields) {
      structField =>
        val searchField = searchFields(structField.name)
        if (featureAssertion.refersToDisablingFeature) {
          featureAssertion.getFeatureValue(searchField) shouldBe Some(false)
        } else {
          featureAssertion.getFeatureValue(searchField) shouldBe Some(true)
        }
    }

    forAll(nonMatchingFields) {
      structField =>
        val searchField = searchFields(structField.name)
        featureAssertion.getFeatureValue(searchField) shouldBe None
    }
  }

  describe(anInstanceOf[SearchFieldsCreationOptions]) {
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

      describe("evaluate and disable enable a feature for fields in a schema") {
        it("facetable") {

          assertFeatureStatusesFor(
            FeatureAssertion.FACETABLE,
            "key",
            Seq(second)
          )
        }

        it("filterable") {

          assertFeatureStatusesFor(
            FeatureAssertion.FILTERABLE,
            "key",
            Seq(first)
          )
        }

        it("hidden") {

          assertFeatureStatusesFor(
            FeatureAssertion.HIDDEN,
            "key",
            Seq(third)
          )
        }

        it("key") {

          assertFeatureStatusesFor(
            FeatureAssertion.Key,
            first,
            Seq.empty
          )
        }

        it("searchable") {

          assertFeatureStatusesFor(
            FeatureAssertion.SEARCHABLE,
            "key",
            Seq(first)
          )
        }

        it("sortable") {

          assertFeatureStatusesFor(
            FeatureAssertion.SORTABLE,
            "key",
            Seq(second)
          )
        }
      }
    }
  }
}
