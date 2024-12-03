package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.LexicalAnalyzerName
import com.azure.search.documents.models.IndexActionType
import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import com.github.jarol.azure.search.spark.sql.connector.core.config.ConfigException

class WriteConfigSpec
  extends BasicSpec {

  private lazy val emptyConfig: WriteConfig = WriteConfig(Map.empty[String, String])
  private lazy val (keyField, indexActionColumn) = ("hello", "world")

  /**
   * Create a write config key related to search field creation options
   * @param suffix suffix to append
   * @return config key for field creation
   */

  private def fieldOptionKey(suffix: String): String = WriteConfig.FIELD_OPTIONS_PREFIX + suffix

  /**
   * Create a write config key related to search field analyzer options
   * @param suffix suffix to append
   * @return config key for field analyzers
   */

  private def analyzerOptionKey(suffix: String): String = fieldOptionKey(WriteConfig.ANALYZERS_PREFIX + suffix)

  /**
   * Assert that an optional collection of strings is defined and contains the same elements w.r.t an expected set
   * @param actual actual set (optional)
   * @param expected expected set
   */

  private def assertDefinedAndContaining(
                                          actual: Option[Seq[String]],
                                          expected: Seq[String]
                                        ): Unit = {

    actual shouldBe defined
    actual.get should contain theSameElementsAs expected
  }

  /**
   * Create a raw configuration object for a single analyzer
   * <br>
   * The output map will contain the following keys
   *  - analyzer type
   *  - list of fields on which the analyzer should be set
   * @param analyzerName analyzer
   * @param alias analyzer alias
   * @param onFields fields on which the analyzer should be set
   * @return a raw configuration for an analyzer
   */

  final def rawConfigForAnalyzer(
                                  alias: String,
                                  analyzerName: LexicalAnalyzerName,
                                  analyzerType: SearchFieldAnalyzerType,
                                  onFields: Seq[String]
                                ): Map[String, String] = {

    Map(
      analyzerOptionKey(s"$alias.${WriteConfig.NAME_SUFFIX}") -> analyzerName.toString,
      analyzerOptionKey(s"$alias.${WriteConfig.TYPE_SUFFIX}") -> analyzerType.name(),
      analyzerOptionKey(s"$alias.${WriteConfig.ON_FIELDS_SUFFIX}") -> onFields.mkString(",")
    )
  }

  describe(anInstanceOf[WriteConfig]) {
    describe(SHOULD) {
      describe("retrieve") {
        it("the batch size") {

          val batchSize = 25
          emptyConfig.batchSize shouldBe WriteConfig.DEFAULT_BATCH_SIZE_VALUE
          WriteConfig(
            Map(
              WriteConfig.BATCH_SIZE_CONFIG -> s"$batchSize"
            )
          ).batchSize shouldBe batchSize
        }

        it("the index action type") {

          emptyConfig.maybeUserSpecifiedAction shouldBe empty
          emptyConfig.overallAction shouldBe WriteConfig.DEFAULT_ACTION_TYPE
          val action = IndexActionType.UPLOAD
          val configMaps: Seq[Map[String, String]] = Seq(
            action.name(),
            action.toString,
            action.name().toLowerCase,
            action.toString.toUpperCase,
            action.toString.toLowerCase,
          ).map {
            value => Map(
              WriteConfig.ACTION_CONFIG -> value
            )
          }

          forAll(configMaps) {
            configMap =>

              val wConfig = WriteConfig(configMap)
              wConfig.maybeUserSpecifiedAction shouldBe Some(action)
              wConfig.overallAction shouldBe action
          }
        }

        it("the name of the index action type column") {

          val colName = "actionCol"
          emptyConfig.actionColumn shouldBe empty
          WriteConfig(
            Map(
              WriteConfig.INDEX_ACTION_COLUMN_CONFIG -> colName
            )
          ).actionColumn shouldBe Some(colName)
        }

        describe("search field creation options") {
          it("eventually throwing an exception for missing key fields") {

            a[ConfigException] shouldBe thrownBy {
              emptyConfig.searchFieldCreationOptions
            }
          }

          describe("related to") {
            it("field features") {

              val (facetable, filterable) = (Seq("f1"), Seq("f2"))
              val (hidden, searchable, sortable) = (Seq("f3"), Seq("f4"), Seq("f5"))
              val options = WriteConfig(
                Map(
                  fieldOptionKey(WriteConfig.KEY_FIELD_CONFIG) -> keyField,
                  fieldOptionKey(WriteConfig.DISABLE_FACETING_CONFIG) -> facetable.mkString(","),
                  fieldOptionKey(WriteConfig.DISABLE_FILTERING_CONFIG) -> filterable.mkString(","),
                  fieldOptionKey(WriteConfig.HIDDEN_FIELDS_CONFIG) -> hidden.mkString(","),
                  fieldOptionKey(WriteConfig.DISABLE_SEARCH_CONFIG) -> searchable.mkString(","),
                  fieldOptionKey(WriteConfig.DISABLE_SORTING_CONFIG) -> sortable.mkString(","),
                  WriteConfig.INDEX_ACTION_COLUMN_CONFIG -> indexActionColumn
                )
              ).searchFieldCreationOptions

              options.keyField shouldBe keyField
              assertDefinedAndContaining(options.disabledFromFaceting, facetable)
              assertDefinedAndContaining(options.disabledFromFiltering, filterable)
              assertDefinedAndContaining(options.hiddenFields, hidden)
              assertDefinedAndContaining(options.disabledFromSearch, searchable)
              assertDefinedAndContaining(options.disabledFromSorting, sortable)
              options.indexActionColumn shouldBe Some(indexActionColumn)
            }

            it("field analyzers") {

              val aliases: Map[String, (SearchFieldAnalyzerType, LexicalAnalyzerName, Seq[String])] = Map(
                "first" -> (SearchFieldAnalyzerType.SEARCH_AND_INDEX, LexicalAnalyzerName.SIMPLE, Seq("a1", "a2")),
                "second" -> (SearchFieldAnalyzerType.SEARCH, LexicalAnalyzerName.STOP, Seq("a3", "a4")),
                "third" -> (SearchFieldAnalyzerType.INDEX, LexicalAnalyzerName.IT_MICROSOFT, Seq("a5", "a6"))
              )

              val baseMap = Map(
                fieldOptionKey(WriteConfig.KEY_FIELD_CONFIG) -> keyField,
                analyzerOptionKey(WriteConfig.ALIASES_SUFFIX) -> aliases.keySet.mkString(",")
              )

              val rawConfig = aliases.foldLeft(baseMap) {
                case (map, (alias, (analyzerType, name, onFields))) =>
                  map ++ rawConfigForAnalyzer(alias, name, analyzerType, onFields)
              }

              val options = WriteConfig(rawConfig).searchFieldCreationOptions
              options.keyField shouldBe keyField
              options.analyzerConfigs shouldBe defined
              val analyzerConfigs = options.analyzerConfigs.get
              analyzerConfigs should have size aliases.size
              forAll(aliases.toSeq) {
                case (alias, (analyzerType, name, onFields)) =>

                  val maybeAnalyzerConfig = analyzerConfigs.find {
                    _.alias.equalsIgnoreCase(alias)
                  }

                  maybeAnalyzerConfig shouldBe defined
                  val analyzerConfig = maybeAnalyzerConfig.get
                  analyzerConfig.`type` shouldBe analyzerType
                  analyzerConfig.name shouldBe name
                  analyzerConfig.fields should contain theSameElementsAs onFields
              }
            }
          }
        }
      }
    }
  }
}
