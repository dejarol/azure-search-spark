package io.github.dejarol.azure.search.spark.connector.write.config

import com.azure.search.documents.indexes.models.{LexicalAnalyzerName, SearchField}
import io.github.dejarol.azure.search.spark.connector.{BasicSpec, FieldFactory, JsonMixIns}
import io.github.dejarol.azure.search.spark.connector.core.config.SearchConfig
import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters
import io.github.dejarol.azure.search.spark.connector.core.schema.SearchFieldFeature
import org.apache.spark.sql.types.{DataTypes, StructField}

class SearchFieldCreationOptionsSpec
  extends BasicSpec
    with JsonMixIns
      with FieldFactory {

  private lazy val (first, second, third, fourth) = ("first", "second", "third", "fourth")
  private lazy val analyzer = LexicalAnalyzerName.BN_MICROSOFT
  private lazy val analyzerType = SearchFieldAnalyzerType.ANALYZER

  /**
   * Create an instance of [[SearchFieldCreationOptions]]
   * @param config raw configuration map
   * @param indexActionColumn index action column
   * @return an instance for testing
   */

  private def createInstance(
                              config: Map[String, String],
                              indexActionColumn: Option[String]
                            ): SearchFieldCreationOptions = {

    SearchFieldCreationOptions(
      new SearchConfig(config),
      indexActionColumn
    )
  }

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

    val featuresMap = Map(
      SearchFieldCreationOptions.KEY_FIELD_CONFIG -> Some(Seq(key)),
      SearchFieldCreationOptions.NON_FACETABLE_CONFIG -> actions.get(SearchFieldFeature.FACETABLE),
      SearchFieldCreationOptions.NON_FILTERABLE_CONFIG -> actions.get(SearchFieldFeature.FILTERABLE),
      SearchFieldCreationOptions.HIDDEN_FIELDS_CONFIG -> actions.get(SearchFieldFeature.HIDDEN),
      SearchFieldCreationOptions.NON_SEARCHABLE_CONFIG -> actions.get(SearchFieldFeature.SEARCHABLE),
      SearchFieldCreationOptions.NON_SORTABLE_CONFIG -> actions.get(SearchFieldFeature.SORTABLE)
    ).collect {
      case (key, Some(values)) => (key, values.mkString(","))
    }

    createInstance(featuresMap, indexActionColumn)
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
   * Creates an AnalyzerConfig object with the specified analyzer name, type, and fields.
   * @param name analyzer name
   * @param tpe analyzer type
   * @param fields field names
   * @return An AnalyzerConfig object containing the specified analyzer configuration.
   */

  private def createAnalyzerConfig(
                                    name: LexicalAnalyzerName,
                                    tpe: SearchFieldAnalyzerType,
                                    fields: Seq[String]
                                  ): AnalyzerConfig = {

    new AnalyzerConfig(
      name,
      tpe,
      JavaScalaConverters.seqToList(fields)
    )
  }

  /**
   * Create an instance of [[SearchFieldCreationOptions]], providing only some [[AnalyzerConfig]]
   * @param analyzerConfigs collection of analyzer configurations
   * @return an instance of [[SearchFieldCreationOptions]]
   */

  private def createAnalyzerOptions(analyzerConfigs: Seq[AnalyzerConfig]): SearchFieldCreationOptions = {

    val analyzerConfig = Map(
      SearchFieldCreationOptions.KEY_FIELD_CONFIG -> "key",
      SearchFieldCreationOptions.ANALYZERS_CONFIG -> writeValueAs[Seq[AnalyzerConfig]](analyzerConfigs)
    )

    createInstance(analyzerConfig, None)
  }

  describe(anInstanceOf[SearchFieldCreationOptions]) {
    describe(SHOULD) {
      it("retrieve user-defined key field or a default value") {

        // user-defined key field
        val keyField = "uuid"
        createInstance(
          Map(
            SearchFieldCreationOptions.KEY_FIELD_CONFIG -> keyField
          ),
          None
        ).keyField shouldBe keyField

        // default key field
        createInstance(Map.empty, None).keyField shouldBe SearchFieldCreationOptions.DEFAULT_KEY_FIELD_CONFIG_VALUE
      }

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
                  createAnalyzerConfig(analyzer, analyzerType, Seq(second))
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
                  createAnalyzerConfig(analyzer, analyzerType, Seq(s"$first.$second"))
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
