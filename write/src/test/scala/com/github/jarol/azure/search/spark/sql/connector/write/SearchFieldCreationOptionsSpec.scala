package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.{LexicalAnalyzerName, SearchField}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.SearchFieldFeature
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, FieldFactory}
import org.apache.spark.sql.types.{DataTypes, StructField}

class SearchFieldCreationOptionsSpec
  extends BasicSpec
    with FieldFactory {

  private lazy val (first, second, third, fourth) = ("first", "second", "third", "fourth")
  private lazy val analyzer = LexicalAnalyzerName.BN_MICROSOFT
  private lazy val analyzerType = SearchFieldAnalyzerType.SEARCH_AND_INDEX

  /**
   * Create a map with keys being all values from enum [[SearchFieldFeature]], and values the given list of string
   * @param fields list of field names
   * @return a map for enabling/disabling all defined features for given fields
   */

  private def createCompleteFeaturesMap(fields: Seq[String]): Map[SearchFieldFeature, Seq[String]] = {

    SearchFieldFeature.values().map {
      v => (v, fields)
    }.toMap
  }

  /**
   * Create an instance of [[SearchFieldCreationOptions]]
   * @param key name of key field
   * @param actions feature
   * @param indexActionColumn index action column
   * @return an instance of [[SearchFieldCreationOptions]]
   */

  private def createFeatureOptions(
                                    key: String,
                                    actions: Map[SearchFieldFeature, Seq[String]],
                                    indexActionColumn: Option[String]
                                  ): SearchFieldCreationOptions = {

    SearchFieldCreationOptions(
      key,
      disabledFromFiltering = actions.get(SearchFieldFeature.FILTERABLE),
      disabledFromSorting = actions.get(SearchFieldFeature.SORTABLE),
      hiddenFields = actions.get(SearchFieldFeature.HIDDEN),
      disabledFromSearch = actions.get(SearchFieldFeature.SEARCHABLE),
      disabledFromFaceting = actions.get(SearchFieldFeature.FACETABLE),
      None,
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
                                  options: SearchFieldCreationOptions,
                                  schema: Seq[StructField]
                                ): Map[String, SearchField] = {

    options.toSearchFields(schema)
      .map {
        sf => (sf.getName, sf)
      }.toMap
  }

  /**
   * Create an instance of [[SearchFieldCreationOptions]], providing only some [[AnalyzerConfig]]
   * @param analyzerConfigs collection of analyzer configurations
   * @return an instance of [[SearchFieldCreationOptions]]
   */

  private def createAnalyzerOptions(analyzerConfigs: Seq[AnalyzerConfig]): SearchFieldCreationOptions = {

    SearchFieldCreationOptions(
      "key",
      None,
      None,
      None,
      None,
      None,
      Some(analyzerConfigs),
      None
    )
  }

  describe(anInstanceOf[SearchFieldCreationOptions]) {
    describe(SHOULD) {
      it("not include index action column") {

        val schema = Seq(
          createStructField(first, DataTypes.StringType),
          createStructField(second, DataTypes.IntegerType),
          createStructField(third, DataTypes.DateType)
        )

        val indexActionColumn = fourth
        val input = schema :+ createStructField(indexActionColumn, DataTypes.StringType)
        val actual = createFeatureOptions("key", Map.empty, Some(fourth))
          .excludeIndexActionColumn(schema)

        actual should not be empty
        val expectedFieldNames = input.collect {
          case sf if !sf.name.equalsIgnoreCase(indexActionColumn) => sf.name
        }

        val actualFieldNames = actual.map(_.name)
        actualFieldNames should contain theSameElementsAs expectedFieldNames
      }

      describe("apply some actions to Search fields, like") {
        describe("enabling or disabling a feature on") {
          it("a top-level field") {

            val schema = createStructType(
              createStructField(first, DataTypes.StringType),
              createStructField(second, DataTypes.StringType),
              createStructField(third, DataTypes.StringType)
            )

            val options = createFeatureOptions(first, createCompleteFeaturesMap(Seq(second)), None)
            val searchFields = getSearchFieldsMap(options, schema)
            searchFields(first) shouldBe enabledFor(SearchFieldFeature.KEY)
            val matchingField = searchFields(second)
            matchingField should not be enabledFor(SearchFieldFeature.FACETABLE)
            matchingField should not be enabledFor(SearchFieldFeature.FILTERABLE)
            matchingField shouldBe enabledFor(SearchFieldFeature.HIDDEN)
            matchingField should not be enabledFor(SearchFieldFeature.SEARCHABLE)
            matchingField should not be enabledFor(SearchFieldFeature.SORTABLE)
          }

          it("a nested field") {

            val schema = createStructType(
              createStructField(first, DataTypes.StringType),
              createStructField(
                second,
                createStructType(
                  createStructField(third, DataTypes.StringType),
                  createStructField("code", DataTypes.IntegerType)
                )
              )
            )

            val options = createFeatureOptions(first, createCompleteFeaturesMap(Seq(s"$second.$third")), None)
            val searchFields = getSearchFieldsMap(options, schema)
            searchFields(first) shouldBe enabledFor(SearchFieldFeature.KEY)
            val maybeSubField = maybeGetSubField(searchFields, second, third)
            maybeSubField shouldBe defined
            val subField = maybeSubField.get
            subField should not be enabledFor(SearchFieldFeature.FACETABLE)
            subField should not be enabledFor(SearchFieldFeature.FILTERABLE)
            subField shouldBe enabledFor(SearchFieldFeature.HIDDEN)
            subField should not be enabledFor(SearchFieldFeature.SEARCHABLE)
            subField should not be enabledFor(SearchFieldFeature.SORTABLE)
          }
        }

        describe("setting an analyzer on") {
          it("top-level fields") {

            val schema = createStructType(
              createStructField(first, DataTypes.StringType),
              createStructField(second, DataTypes.StringType)
            )


            val searchFields = getSearchFieldsMap(
              createAnalyzerOptions(
                Seq(
                  AnalyzerConfig("hello", analyzer, analyzerType, Seq(second))
                )
              ),
              schema
            )

            searchFields should have size schema.size
            analyzerType.getFromField(searchFields(first)) shouldBe null
            analyzerType.getFromField(searchFields(second)) shouldBe analyzer
          }

          it("nested fields") {

            val schema = createStructType(
              createStructField(
                first,
                createStructType(
                  createStructField(second, DataTypes.StringType),
                  createStructField(third, DataTypes.IntegerType)
                )
              )
            )

            val searchFields = getSearchFieldsMap(
              createAnalyzerOptions(
                Seq(
                  AnalyzerConfig("hello", analyzer, analyzerType, Seq(s"$first.$second"))
                )
              ),
              schema
            )

            searchFields should have size schema.size
            searchFields(first).getAnalyzerName shouldBe null
            val maybeSubField = maybeGetSubField(searchFields, first, second)
            maybeSubField shouldBe defined
            val subField = maybeSubField.get
            subField.getAnalyzerName shouldBe analyzer
          }
        }
      }
    }
  }
}
