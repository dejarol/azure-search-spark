package com.github.jarol.azure.search.spark.sql.connector.core

import com.github.jarol.azure.search.spark.sql.connector.core.utils.StringUtils

/**
 * Mix-in trait for specs that deal with JSON representing Azure Search REST API models
 */

trait SearchAPIModelFactory {

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

    s"""{
       |  "${TestConstants.ODATA_TYPE}": "${TestConstants.BM25_SIMILARITY_ALGORITHM}",
       |  "k1": $k1,
       |  "b": $b
       |}""".stripMargin
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

    s"""
       |{
       |  "${TestConstants.ODATA_TYPE}": "${TestConstants.CLASSIC_TOKENIZER}",
       |  "name": "$name",
       |  "maxTokenLength": $maxTokenLength
       |}""".stripMargin
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
    s"""
       |{
       |  "name": "$name",
       |  "sourceFields": ${createArrayOfStrings(fields)}
       |}""".stripMargin
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

    s"""
       |{
       |  "${TestConstants.ODATA_TYPE}": "#Microsoft.Azure.Search.StopAnalyzer",
       |  "name": "$name",
       |  "stopwords": ${createArrayOfStrings(stopWords)}
       |}""".stripMargin
  }

  protected final def createMappingCharFilter(
                                               name: String,
                                               mappings: Seq[String]
                                             ): String = {

    s"""
       |{
       | "${TestConstants.ODATA_TYPE}": "#Microsoft.Azure.Search.MappingCharFilter",
       | "name": "$name",
       | "mappings": ${createArrayOfStrings(mappings)}
       |}""".stripMargin
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

    val weightsMap = weights.map {
      case (k, v) => s"${StringUtils.quoted(k)}: $v"
    }.mkString("{", ",", "}")

    s"""
       |{
       |  "name": ${StringUtils.quoted(name)},
       |  "text": {
       |    "weights": $weightsMap
       |  }
       |}""".stripMargin
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

    s"""
       |{
       |  "${TestConstants.ODATA_TYPE}": "#Microsoft.Azure.Search.PatternReplaceTokenFilter",
       |  "name": "$name",
       |  "pattern": "$pattern",
       |  "replacement": "$replacement"
       |}""".stripMargin
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

    s"""
       |{
       |  "allowedOrigins": ${createArrayOfStrings(allowedOrigins)},
       |  "maxAgeInSeconds": $maxAgeInSeconds
       |}""".stripMargin
  }
}
