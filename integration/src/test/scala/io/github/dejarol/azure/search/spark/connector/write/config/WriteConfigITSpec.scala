package io.github.dejarol.azure.search.spark.connector.write.config

import com.azure.search.documents.indexes.models._
import io.github.dejarol.azure.search.spark.connector.core.schema.GeoPointType
import io.github.dejarol.azure.search.spark.connector.models.SimpleBean
import io.github.dejarol.azure.search.spark.connector.{FieldAssertionMixins, SearchITSpec}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.scalatest.BeforeAndAfterEach

import java.util.{List => JList}

class WriteConfigITSpec
  extends SearchITSpec
    with WriteConfigFactory
      with BeforeAndAfterEach
       with FieldAssertionMixins {

  private lazy val (idFieldName, testIndex) = ("id", "write-builder-index")
  private lazy val keyField = createStructField(idFieldName, DataTypes.StringType)
  private lazy val analyzer = LexicalAnalyzerName.STANDARD_ASCII_FOLDING_LUCENE
  private lazy val minimumOptionsForIndexCreation = optionsForAuthAndIndex(testIndex) + (
    fieldOptionKey(idFieldName) ->
      s"""
         |{
         |  "key": true
         |}
         |""".stripMargin
    )

  private lazy val uuidFieldName = "uuid"
  private lazy val (parent, subFieldName) = ("parent", "child")
  private lazy val schemaForAnalyzerTests = createStructType(
    keyField,
    createStructField(uuidFieldName, DataTypes.StringType),
    createStructField(
      parent,
      createStructType(
        createStructField(subFieldName, DataTypes.StringType),
        createStructField("other", DataTypes.TimestampType)
      )
    )
  )

  /**
   * Delete index used for integration testing
   */

  override def afterEach(): Unit = {

    dropIndexIfExists(testIndex, sleep = true)
    super.afterEach()
  }

  /**
   * Safely create an index, creating a [[WriteConfig]] instance based on provided options
   * @param schema schem for setting the index fields
   * @param writeConfigOptions write configuration options
   * @return
   */

  private def createIndexUsingOptions(
                                       schema: StructType,
                                       writeConfigOptions: Map[String, String]
                                     ): SearchIndex = {

    // Take options for auth and index,
    // add key field and provided options
    val outputValue = WriteConfig(writeConfigOptions)
      .createIndex(testIndex, schema)
    Thread.sleep(3000)
    outputValue
  }

  /**
   * Create an index, creating a [[WriteConfig]] instance. Provided options will be added on top of
   * the minimum set of index creation options
   * @param schema schema for setting the index fields
   * @param extraOptions options to add upon the minimum set required for index creation
   */

  private def createIndexAddingOptions(
                                        schema: StructType,
                                        extraOptions: Map[String, String]
                                      ): SearchIndex = {

    // Trigger the index creation, by adding given options
    // to the minimum set of index options
    createIndexUsingOptions(
      schema,
      minimumOptionsForIndexCreation ++ extraOptions
    )
  }

  /**
   * Assert that a field has been properly enabled/disabled when creating a new index
   * @param schema schema for index creation
   * @param extraOptions extra options for index creation
   */

  private def createIndexByDisablingFeatures(
                                              schema: StructType,
                                              extraOptions: Map[String, String]
                                            ): Map[String, SearchField] = {


    indexExists(testIndex) shouldBe false

    // Create index, assert existence
    createIndexAddingOptions(schema, extraOptions)
    indexExists(testIndex) shouldBe true

    // Retrieve index fields
    getIndexFieldsAsMap(testIndex)
  }

  /**
   * Create a Search index, setting some field analyzers, and get back the list of generated Search fields
   * @param analyzerOptions options for setting analyzers
   * @return fields from the newly created Search index
   */

  private def createIndexSettingAnalyzers(analyzerOptions: Map[String, String]): Map[String, SearchField] = {

    indexExists(testIndex) shouldBe false

    // Create index, assert existence
    createIndexAddingOptions(schemaForAnalyzerTests, analyzerOptions)
    indexExists(testIndex) shouldBe true

    // Retrieve index fields
    getIndexFieldsAsMap(testIndex)
  }

  /**
   * Assert that the definition of newly created index has been enriched with some specifications
   * @param options additional options for index creation
   * @param getter function for retrieving the specification definition from the Search index definition
   * @param assertion assertion to run on retrieved specification
   * @tparam A specification type
   * @since 0.11.0
   */

  private def assertIndexHasBeenEnrichedWith[A](
                                                 options: Map[String, String],
                                                 getter: SearchIndex => A
                                               )(
                                                 assertion: A => Unit
                                               ): Unit = {

    val searchIndexObj = createIndexAddingOptions(
      schemaForAnalyzerTests,
      options
    )

    // Retrieve the index specification and run assertion
    assertion(
      getter(searchIndexObj)
    )
  }

  /**
   * Assert that the definition of newly created index has been enriched with some specifications
   * @param key key (to be added to a [[WriteConfig]] instance) related to the specification
   * @param value specification value
   * @param getter function for retrieving the specification definition from the Search index definition
   * @param assertion assertion to run on retrieved specification
   * @tparam A specification type
   */

  private def assertIndexHasBeenEnrichedWith[A](
                                                 key: String,
                                                 value: String,
                                                 getter: SearchIndex => A
                                               )(
                                                 assertion: A => Unit
                                               ): Unit = {
    assertIndexHasBeenEnrichedWith[A](
      options = Map(indexOptionKey(key) -> value),
      getter = getter
    ) {
      assertion
    }
  }

  /**
   * Create an index using a given schema, setting a geo conversion exclusion
   * @param schema schema to use for index creation
   * @param exclusion name of the column to exclude from natural geo conversion
   * @return a map of index fields (keys are field names and values the field themselves)
   */

  private def createIndexExcludingGeoColumn(
                                             schema: StructType,
                                             exclusion: String
                                           ): Map[String, SearchField] = {

    // Create an index by setting geo conversion exclusion
    createIndexAddingOptions(
      schema,
      Map(
        WriteConfig.EXCLUDE_FROM_GEO_CONVERSION_CONFIG -> exclusion
      )
    )

    getIndexFieldsAsMap(testIndex)
  }

  describe(anInstanceOf[WriteConfig]) {
    describe(SHOULD) {
      it("drop an existing index") {

        val indexName = "simple-beans"
        createIndexFromSchemaOf[SimpleBean](indexName)
        indexExists(indexName) shouldBe true

        WriteConfig(optionsForAuth).deleteIndex(indexName)
        indexExists(indexName) shouldBe false
      }

      describe("create an index from a name and a schema") {
        it("with as many fields as many columns") {

          val schema = createStructType(
            keyField,
            createStructField("name", DataTypes.StringType),
            createStructField("date", DataTypes.TimestampType),
            createArrayField("education",
              createStructType(
                createStructField("city", DataTypes.StringType),
                createStructField("title", DataTypes.StringType),
                createStructField("grade", DataTypes.IntegerType)
              )
            )
          )

          indexExists(testIndex) shouldBe false
          createIndexAddingOptions(schema, Map.empty)
          indexExists(testIndex) shouldBe true
          assertMatchBetweenSchemaAndIndex(schema, testIndex)
        }

        it("not including the column used for index action type") {

          val actionTypeColName = "actionType"
          val schema = createStructType(
            keyField,
            createStructField("value", DataTypes.LongType),
            createStructField(actionTypeColName, DataTypes.StringType)
          )

          indexExists(testIndex) shouldBe false
          createIndexAddingOptions(
            schema,
            Map(
              WriteConfig.INDEX_ACTION_COLUMN_CONFIG -> actionTypeColName
            )
          )
          indexExists(testIndex) shouldBe true
          val actualFields = getIndexFieldsAsMap(testIndex)

          val expectedSchema = schema.filterNot {
            _.name.equalsIgnoreCase(actionTypeColName)
          }

          actualFields should have size expectedSchema.size
          actualFields.keySet should contain theSameElementsAs expectedSchema.map(_.name)
        }

        it(s"automatically setting the ${SearchFieldCreationOptions.DEFAULT_ID_COLUMN} field as the index key") {

          val schema = createStructType(
            keyField,
            createStructField("name", DataTypes.StringType),
            createStructField("value", DataTypes.LongType)
          )

          // Use the minimum set of index creation options,
          // letting the connector automatically set the index key
          indexExists(testIndex) shouldBe false
          createIndexUsingOptions(
            schema,
            optionsForAuthAndIndex(testIndex)
          )
          indexExists(testIndex) shouldBe true
          val actualFields = getIndexFieldsAsMap(testIndex)
          val maybeKeyField = actualFields.get(keyField.name)
          maybeKeyField shouldBe defined
          maybeKeyField.get.isKey shouldBe true
        }

        describe("enriching fields with") {
          describe("some features, like") {
            it("facetable") {

              val fieldName = "category"
              val nonFacetableField = createStructField(fieldName, DataTypes.StringType)
              val schema = createStructType(
                keyField,
                createStructField("discount", DataTypes.DoubleType),
                nonFacetableField
              )

              val options = Map(
                fieldOptionKey(fieldName) ->
                  s"""
                     |{
                     |  "facetable": false
                     |}
                     |""".stripMargin
              )

              val fields = createIndexByDisablingFeatures(schema, options)
              fields(fieldName).isFacetable shouldBe false
            }

            it("filterable") {

              val fieldName = "level"
              val nonFilterableField = createStructField(fieldName, DataTypes.IntegerType)
              val schema = createStructType(
                keyField,
                nonFilterableField,
                createStructField("date", DataTypes.TimestampType)
              )

              val options = Map(
                fieldOptionKey(fieldName) ->
                  s"""
                     |{
                     |  "filterable": false
                     |}
                     |""".stripMargin
              )

              val fields = createIndexByDisablingFeatures(schema, options)
              fields(fieldName).isFilterable shouldBe false
            }

            it("hidden") {

              val firstHidden = createStructField("first", DataTypes.IntegerType)
              val secondHidden = createStructField("second", DataTypes.TimestampType)
              val schema = createStructType(
                keyField,
                firstHidden,
                secondHidden,
                createStructField("category", DataTypes.StringType)
              )

              val options = Seq(firstHidden, secondHidden).map {
                field => (
                  fieldOptionKey(field.name),
                  s"""
                     |{
                     |  "retrievable": false
                     |}
                     |""".stripMargin
                )
              }.toMap

              val fields = createIndexByDisablingFeatures(schema, options)
              fields(firstHidden.name).isHidden shouldBe true
              fields(secondHidden.name).isHidden shouldBe true
            }

            it("searchable") {

              val fieldName = "description"
              val nonSearchableField = createStructField(fieldName, DataTypes.StringType)
              val schema = createStructType(
                keyField,
                nonSearchableField,
                createStructField("date", DataTypes.DateType)
              )

              val options = Map(
                fieldOptionKey(fieldName) ->
                  s"""
                     |{
                     |  "searchable": false
                     |}
                     |""".stripMargin
              )

              val fields = createIndexByDisablingFeatures(schema, options)
              fields(fieldName).isSearchable shouldBe false
            }

            it("sortable") {

              val fieldName = "level"
              val nonSortableField = createStructField(fieldName, DataTypes.IntegerType)
              val schema = createStructType(
                keyField,
                nonSortableField,
                createStructField(
                  "address",
                  createStructType(
                    createStructField("city", DataTypes.StringType)
                  )
                )
              )

              val options = Map(
                fieldOptionKey(fieldName) ->
                  s"""
                     |{
                     |  "sortable": false
                     |}
                     |""".stripMargin
              )

              val fields = createIndexByDisablingFeatures(schema, options)
              fields(fieldName).isSortable shouldBe false
            }
          }

          describe("analyzers for") {
            it("both searching and indexing") {

              val fieldsToEnrich = Seq(uuidFieldName, s"$parent.$subFieldName")
              val analyzersOptions: Map[String, String] = fieldsToEnrich.map {
                field =>
                  fieldOptionKey(field) ->
                    s"""
                       |{
                       |  "analyzer": "$analyzer"
                       |}
                       |""".stripMargin
              }.toMap

              val searchFields = createIndexSettingAnalyzers(analyzersOptions)
              searchFields(uuidFieldName).getAnalyzerName shouldBe analyzer
              val maybeSubField = maybeGetSubField(searchFields, parent, subFieldName)
              maybeSubField shouldBe defined
              val subFieldDefinition = maybeSubField.get
              subFieldDefinition.getAnalyzerName shouldBe analyzer
            }

            it("only search or indexing") {

              val fieldsToEnrich = Seq(uuidFieldName, s"$parent.$subFieldName")
              val (indexAnalyzer, searchAnalyzer) = (LexicalAnalyzerName.SIMPLE, LexicalAnalyzerName.STOP)
              val analyzerOptions = fieldsToEnrich.map {
                field =>
                  fieldOptionKey(field) ->
                    s"""
                       |{
                       |  "indexAnalyzer": "$indexAnalyzer",
                       |  "searchAnalyzer": "$searchAnalyzer"
                       |}
                       |""".stripMargin
              }.toMap

              val searchFields = createIndexSettingAnalyzers(analyzerOptions)
              val uuidField = searchFields(uuidFieldName)
              val maybeSubField = maybeGetSubField(searchFields, parent, subFieldName)
              maybeSubField shouldBe defined
              val subField = maybeSubField.get

              forAll(
                Seq(uuidField, subField)
              ) {
                field =>
                  field.getIndexAnalyzerName shouldBe indexAnalyzer
                  field.getSearchAnalyzerName shouldBe searchAnalyzer
              }
            }
          }
        }

        describe("taking into account user-specific settings, like") {
          describe("excluding from geo conversion") {

            val (locationColName, addressColName) = ("location", "address")
            val exclusionForNestedField = s"$addressColName.$locationColName"
            val idField = createStructField(idFieldName, DataTypes.StringType)
            val locationField = createStructField(
              locationColName,
              GeoPointType.SPARK_SCHEMA
            )

            val locationArrayField = createStructField(
              locationColName,
              createArrayType(GeoPointType.SPARK_SCHEMA)
            )

            /**
             * Extract the name and datatype of each field
             * @param fields map of fields
             * @return map of field name and datatype
             */

            def nameAndDatatypeOf(fields: Map[String, SearchField]): Map[String, SearchFieldDataType] = {

              fields.mapValues {
                _.getType
              }
            }

            it("top-level fields") {

              // Create an index by setting geo conversion exclusion for 'geoColumn'
              val schema = createStructType(idField, locationField)
              val actual = createIndexExcludingGeoColumn(schema, locationColName)
              actual should have size schema.size
              nameAndDatatypeOf(actual) should contain theSameElementsAs Map(
                idFieldName -> SearchFieldDataType.STRING,
                locationColName -> SearchFieldDataType.COMPLEX
              )

              // Assert that the 'geoColumn' field is complex with geo point structure
              assertIsComplexWithGeopointStructure(
                actual(locationColName)
              )
            }

            it("top-level collection fields") {

              // Create an index by setting geo conversion exclusion for 'geoColumn'
              val schema = createStructType(idField, locationArrayField)
              val actual = createIndexExcludingGeoColumn(schema, locationColName)
              actual should have size schema.size
              nameAndDatatypeOf(actual) should contain theSameElementsAs Map(
                idFieldName -> SearchFieldDataType.STRING,
                locationColName -> SearchFieldDataType.collection(
                  SearchFieldDataType.COMPLEX
                )
              )

              // Assert that the geoColumn is complex with geo point structure
              assertIsComplexWithGeopointStructure(
                actual(locationColName)
              )
            }

            it("nested fields") {

              val schema = createStructType(
                idField,
                createStructField(
                  addressColName,
                  createStructType(
                    locationField
                  )
                )
              )

              val actual = createIndexExcludingGeoColumn(schema, exclusionForNestedField)
              actual should have size schema.size
              nameAndDatatypeOf(actual) should contain theSameElementsAs Map(
                idFieldName -> SearchFieldDataType.STRING,
                addressColName -> SearchFieldDataType.COMPLEX
              )

              val addressSubFields = actual(addressColName).getFields
              assertAllFieldsMatchNameAndDatatype(
                addressSubFields,
                Map(
                  locationColName -> SearchFieldDataType.COMPLEX
                )
              )

              // Assert that the geoColumn is complex with geo point structure
              assertIsComplexWithGeopointStructure(
                addressSubFields.get(0)
              )
            }

            it("nested collection fields") {

              val schema = createStructType(
                idField,
                createStructField(
                  addressColName,
                  createStructType(
                    createStructField(
                      locationColName,
                      createArrayType(GeoPointType.SPARK_SCHEMA)
                    )
                  )
                )
              )

              val actual = createIndexExcludingGeoColumn(schema, exclusionForNestedField)
              actual should have size schema.size
              nameAndDatatypeOf(actual) should contain theSameElementsAs Map(
                idFieldName -> SearchFieldDataType.STRING,
                addressColName -> SearchFieldDataType.COMPLEX
              )

              val addressSubFields = actual(addressColName).getFields
              assertAllFieldsMatchNameAndDatatype(
                addressSubFields,
                Map(
                  locationColName -> SearchFieldDataType.collection(
                    SearchFieldDataType.COMPLEX
                  )
                )
              )

              // Assert that the geoColumn is complex with geo point structure
              assertIsComplexWithGeopointStructure(
                addressSubFields.get(0)
              )
            }
          }
        }

        describe("enriching its definition with") {
          it("a similarity algorithm") {

            val (k1, b) = (1.5, 0.8)
            assertIndexHasBeenEnrichedWith[SimilarityAlgorithm](
              SearchIndexEnrichmentOptions.SIMILARITY_CONFIG,
              createBM25SimilarityAlgorithm(k1, b),
              (index: SearchIndex) => index.getSimilarity
            ) {
              algo =>
                algo shouldBe a [BM25SimilarityAlgorithm]
                val bm25 = algo.asInstanceOf[BM25SimilarityAlgorithm]
                bm25.getK1 shouldBe k1
                bm25.getB shouldBe b
            }
          }

          it("some tokenizers") {

            val (name, maxTokenLength) = ("tokenizrName", 10)
            assertIndexHasBeenEnrichedWith[JList[LexicalTokenizer]](
              SearchIndexEnrichmentOptions.TOKENIZERS_CONFIG,
              createArray(
                createClassicTokenizer(name, maxTokenLength)
              ),
              (index: SearchIndex) => index.getTokenizers
            ) {
              tokenizers =>
                tokenizers should have size 1
                val head = tokenizers.get(0)
                head shouldBe a [ClassicTokenizer]
                val clsTokenizer = head.asInstanceOf[ClassicTokenizer]
                clsTokenizer.getName shouldBe name
                clsTokenizer.getMaxTokenLength shouldBe maxTokenLength
            }
          }

          it("search suggesters") {

            val (name, fields) = ("uuidSuggstr", Seq(uuidFieldName))
            assertIndexHasBeenEnrichedWith[JList[SearchSuggester]](
              SearchIndexEnrichmentOptions.SUGGESTERS_CONFIG,
              createArray(
                createSearchSuggester(name, fields)
              ),
              (index: SearchIndex) => index.getSuggesters
            ) {
              suggesters =>
                suggesters should have size 1
                val head = suggesters.get(0)
                head.getName shouldBe name
                head.getSourceFields should contain theSameElementsAs fields
            }
          }

          it("analyzers") {

            val (name, stopWords) = ("analyzrName", Seq("a", "the"))
            assertIndexHasBeenEnrichedWith[JList[LexicalAnalyzer]](
              SearchIndexEnrichmentOptions.ANALYZERS_CONFIG,
              createArray(
                createStopAnalyzer(name, stopWords)
              ),
              (index: SearchIndex) => index.getAnalyzers
            ) {
              analyzers =>
                analyzers should have size 1
                val head = analyzers.get(0)
                head shouldBe a [StopAnalyzer]
                val stopAnalyzer = head.asInstanceOf[StopAnalyzer]
                stopAnalyzer.getName shouldBe name
                stopAnalyzer.getStopwords should contain theSameElementsAs stopWords
            }
          }

          it("char filters") {

            val (name, mappings) = ("filterName", Seq("john=>jane"))
            assertIndexHasBeenEnrichedWith[JList[CharFilter]](
              SearchIndexEnrichmentOptions.CHAR_FILTERS_CONFIG,
              createArray(
                createMappingCharFilter(name, mappings)
              ),
              (index: SearchIndex) => index.getCharFilters
            ) {
              charFilters =>
                charFilters should have size 1
                val head = charFilters.get(0)
                head shouldBe a [MappingCharFilter]
                val mappingCharFilter = head.asInstanceOf[MappingCharFilter]
                mappingCharFilter.getName shouldBe name
                mappingCharFilter.getMappings should contain theSameElementsAs mappings
            }
          }

          it("scoring profiles") {

            val (name, weights) = ("customScoring1", Map(uuidFieldName -> 0.5))
            assertIndexHasBeenEnrichedWith[JList[ScoringProfile]](
              SearchIndexEnrichmentOptions.SCORING_PROFILES_CONFIG,
              createArray(
                createScoringProfile(name, weights)
              ),
              (index: SearchIndex) => index.getScoringProfiles
            ) {
              profiles =>
                profiles should have size 1
                val head = profiles.get(0)
                head.getName shouldBe name
                val actualWeights = head.getTextWeights.getWeights
                forAll(weights.keySet) {
                  k =>
                    actualWeights should contain key k
                    actualWeights.get(k) shouldBe weights(k)
                }
            }
          }

          it("token filters") {

            val (name, pattern, replacement) = ("tfName", "a", "")
            assertIndexHasBeenEnrichedWith[JList[TokenFilter]](
              SearchIndexEnrichmentOptions.TOKEN_FILTERS_CONFIG,
              createArray(
                createPatternReplaceTokenFilter(name, pattern, replacement)
              ),
              (index: SearchIndex) => index.getTokenFilters
            ) {
              filters =>
                filters should have size 1
                val head = filters.get(0)
                head shouldBe a [PatternReplaceTokenFilter]
                val filter = head.asInstanceOf[PatternReplaceTokenFilter]
                filter.getName shouldBe name
                filter.getPattern shouldBe pattern
                filter.getReplacement shouldBe replacement
            }
          }

          it("CORS options") {

            val (allowedOrigins, maxAge) = (Seq("*"), 15)
            assertIndexHasBeenEnrichedWith[CorsOptions](
              SearchIndexEnrichmentOptions.CORS_OPTIONS_CONFIG,
              createCorsOptions(allowedOrigins, maxAge),
              (index: SearchIndex) => index.getCorsOptions
            ) {
              cors =>
                cors.getAllowedOrigins should contain theSameElementsAs allowedOrigins
                cors.getMaxAgeInSeconds shouldBe maxAge
            }
          }

          it("default scoring profile") {

            val (name, weights) = ("customScoring1", Map(uuidFieldName -> 0.5))
            assertIndexHasBeenEnrichedWith[(String, JList[ScoringProfile])](
              Map(
                indexOptionKey(SearchIndexEnrichmentOptions.SCORING_PROFILES_CONFIG) -> createArray(createScoringProfile(name, weights)),
                indexOptionKey(SearchIndexEnrichmentOptions.DEFAULT_SCORING_PROFILE_CONFIG) -> name
              ),
              (idx: SearchIndex) => (
                idx.getDefaultScoringProfile,
                idx.getScoringProfiles
              )
            ) {
              case (actualName, scoringProfiles) =>
                actualName shouldBe name
                scoringProfiles should have size 1
            }
          }

          it("vector search") {

            assertIndexHasBeenEnrichedWith[VectorSearch](
              SearchIndexEnrichmentOptions.VECTOR_SEARCH_CONFIG,
              createVectorSearch(
                Seq(
                  createHnswAlgorithm("hnsw-1", 4, 400, 500, VectorSearchAlgorithmMetric.COSINE)
                ),
                Seq(
                  createVectorSearchProfile("vector-profile-1", "hnsw-1")
                )
              ),
              (index: SearchIndex) => index.getVectorSearch
            ) {
              vectorSearch =>
                vectorSearch.getAlgorithms should have size 1
                vectorSearch.getProfiles should have size 1
            }
          }
        }
      }
    }
  }
}
