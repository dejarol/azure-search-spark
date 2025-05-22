package io.github.dejarol.azure.search.spark.connector

import com.azure.json.{JsonProviders, JsonSerializable, JsonWriter}
import com.azure.search.documents.indexes.models.{BM25SimilarityAlgorithm, CharFilter, ClassicTokenizer, CorsOptions, HnswAlgorithmConfiguration, HnswParameters, LexicalAnalyzer, LexicalTokenizer, MappingCharFilter, PatternReplaceTokenFilter, ScoringProfile, SearchSuggester, SemanticSearch, SimilarityAlgorithm, StopAnalyzer, TextWeights, TokenFilter, VectorSearch, VectorSearchAlgorithmConfiguration, VectorSearchAlgorithmMetric, VectorSearchProfile}
import io.github.dejarol.azure.search.spark.connector.core.{JavaScalaConverters, TestConstants}
import io.github.dejarol.azure.search.spark.connector.core.utils.StringUtils

import java.io.StringWriter
import java.lang.{Double => JDouble}

/**
 * Mix-in trait for specs that deal with JSON representing Azure Search REST API models
 */

trait SearchAPIModelFactory {

  /**
   * Creates the JSON string representation of Search API model
   * @param model an API model instance
   * @tparam T model type
   * @return a JSON string representing the model
   */

  protected final def apiModelToJson[T <: JsonSerializable[T]](model: T): String = {

    val stringWriter = new StringWriter()
    val jsonWriter: JsonWriter = JsonProviders.createWriter(stringWriter)
    model.toJson(jsonWriter)
    jsonWriter.flush()
    stringWriter.toString
  }

  /**
   * Create a JSON array by joining many JSON strings
   * @param elements JSON strings
   * @return a JSON array
   */

  protected final def createArray(elements: String*): String = elements.mkString("[", ",", "]")

  /**
   * Create a JSON array of strings
   * @param elements array strings
   * @return a JSON array of strings
   */

  protected final def createArrayOfStrings(elements: Seq[String]): String = createArray(elements.map(StringUtils.quoted): _*)

  /**
   * Create a JSON string representing a [[com.azure.search.documents.indexes.models.SimilarityAlgorithm]]
   * @param name algorithm name
   * @return a JSON string representing a [[com.azure.search.documents.indexes.models.SimilarityAlgorithm]]
   */

  protected final def createSimpleODataType(name: String): String = {

    s"""
       |{
       | "${TestConstants.ODATA_TYPE}": "$name"
       |}""".stripMargin
  }

  /**
   * Create a JSON representing a valid instance of [[com.azure.search.documents.indexes.models.BM25SimilarityAlgorithm]]
   * @param k1 k1 value
   * @param b b value
   * @return a JSON representing a valid instance of [[com.azure.search.documents.indexes.models.BM25SimilarityAlgorithm]]
   */

  protected final def createBM25SimilarityAlgorithm(
                                                     k1: Double,
                                                     b: Double
                                                   ): String = {

    apiModelToJson[SimilarityAlgorithm](
      new BM25SimilarityAlgorithm()
        .setK1(k1)
        .setB(b)
    )
  }

  /**
   * Create a JSON representing a [[com.azure.search.documents.indexes.models.ClassicTokenizer]]
   * @param name name
   * @param maxTokenLength max token length
   * @return a JSON representing a [[com.azure.search.documents.indexes.models.ClassicTokenizer]]
   */

  protected final def createClassicTokenizer(
                                              name: String,
                                              maxTokenLength: Int
                                            ): String = {

    apiModelToJson[LexicalTokenizer](
      new ClassicTokenizer(name)
        .setMaxTokenLength(maxTokenLength)
    )
  }

  /**
   * Create a JSON representing a [[com.azure.search.documents.indexes.models.SearchSuggester]]
   * @param name name
   * @param fields fields
   * @return a JSON representing a [[com.azure.search.documents.indexes.models.SearchSuggester]]
   */

  protected final def createSearchSuggester(
                                             name: String,
                                             fields: Seq[String]
                                           ): String = {

    apiModelToJson[SearchSuggester](
      new SearchSuggester(
        name,
        JavaScalaConverters.seqToList(fields)
      )
    )
  }

  /**
   * Create a JSON representing a [[com.azure.search.documents.indexes.models.StopAnalyzer]]
   * @param name name
   * @param stopWords stop words
   * @return a JSON representing a [[com.azure.search.documents.indexes.models.StopAnalyzer]]
   */

  protected final def createStopAnalyzer(
                                          name: String,
                                          stopWords: Seq[String]
                                        ): String = {

    apiModelToJson[LexicalAnalyzer](
      new StopAnalyzer(name)
        .setStopwords(stopWords: _*)
    )
  }

  protected final def createMappingCharFilter(
                                               name: String,
                                               mappings: Seq[String]
                                             ): String = {

    apiModelToJson[CharFilter](
      new MappingCharFilter(
        name,
        JavaScalaConverters.seqToList(mappings)
      )
    )
  }

  /**
   * Create a JSON representing a [[com.azure.search.documents.indexes.models.ScoringProfile]]
   * @param name profile name
   * @param weights weights
   * @return a JSON representing a [[com.azure.search.documents.indexes.models.ScoringProfile]]
   */

  protected final def createScoringProfile(
                                            name: String,
                                            weights: Map[String, Double]
                                          ): String = {

    apiModelToJson[ScoringProfile](
      new ScoringProfile(name)
        .setTextWeights(
          new TextWeights(
            JavaScalaConverters.scalaMapToJava(
              weights.mapValues(JDouble.valueOf)
            )
          )
        )
    )
  }

  /**
   * Create a JSON representing a [[com.azure.search.documents.indexes.models.PatternReplaceTokenFilter]]
   * @param name filter name
   * @param pattern pattern to replace
   * @param replacement replacement for pattern occurrences
   * @return a JSON representing a [[com.azure.search.documents.indexes.models.PatternReplaceTokenFilter]]
   */

  protected final def createPatternReplaceTokenFilter(
                                                       name: String,
                                                       pattern: String,
                                                       replacement: String
                                                     ): String = {

    apiModelToJson[TokenFilter](
      new PatternReplaceTokenFilter(
        name,
        pattern,
        replacement
      )
    )
  }

  /**
   * Create a JSON representing a [[com.azure.search.documents.indexes.models.CorsOptions]] instance
   * @param allowedOrigins allowed origins
   * @param maxAgeInSeconds max age in seconds
   * @return a JSON representing a [[com.azure.search.documents.indexes.models.CorsOptions]] instance
   */

  protected final def createCorsOptions(
                                         allowedOrigins: Seq[String],
                                         maxAgeInSeconds: Int
                                       ): String = {

    apiModelToJson[CorsOptions](
      new CorsOptions(
        JavaScalaConverters.seqToList(allowedOrigins)
      ).setMaxAgeInSeconds(maxAgeInSeconds)
    )
  }

  /**
   * Create a [[com.azure.search.documents.indexes.models.VectorSearchProfile]]
   * @param name profile name
   * @param algorithmConfigurationName algorithm configuration name
   * @return a vector search profile
   */

  protected final def createVectorSearchProfile(
                                                 name: String,
                                                 algorithmConfigurationName: String
                                               ): VectorSearchProfile = {

    new VectorSearchProfile(
      name,
      algorithmConfigurationName
    )
  }

  /**
   * Create a [[com.azure.search.documents.indexes.models.HnswAlgorithmConfiguration]] (an algorithm for vector search)
   * @param name algorithm name
   * @param m value of algorithm parameter m
   * @param efConstruction value of algorithm parameter efConstruction
   * @param efSearch value of algorithm parameter efSearch
   * @param metric algorithm metric
   * @return an algorithm for vector search
   */

  protected final def createHnswAlgorithm(
                                           name: String,
                                           m: Int,
                                           efConstruction: Int,
                                           efSearch: Int,
                                           metric: VectorSearchAlgorithmMetric
                                         ): VectorSearchAlgorithmConfiguration = {

    new HnswAlgorithmConfiguration(name)
      .setParameters(
        new HnswParameters()
          .setM(m)
          .setEfConstruction(efConstruction)
          .setEfSearch(efSearch)
          .setMetric(metric)
      )
  }

  /**
   * Create a JSON representing a [[com.azure.search.documents.indexes.models.VectorSearch]] instance
   * @return a JSON representation of a vector search
   */

  protected final def createVectorSearch(
                                          algorithms: Seq[VectorSearchAlgorithmConfiguration],
                                          profiles: Seq[VectorSearchProfile]
                                        ): String = {

    apiModelToJson[VectorSearch](
      new VectorSearch()
        .setAlgorithms(algorithms: _*)
        .setProfiles(profiles: _*)
    )
  }

  /**
   * Create a JSON representing a [[com.azure.search.documents.indexes.models.SemanticSearch]] instance
   * @param configName default configuration name
   * @return a JSON representation of a vector search
   */

  protected final def createSemanticSearch(configName: String): String = {

    apiModelToJson[SemanticSearch](
      new SemanticSearch()
        .setDefaultConfigurationName(configName)
    )
  }
}
