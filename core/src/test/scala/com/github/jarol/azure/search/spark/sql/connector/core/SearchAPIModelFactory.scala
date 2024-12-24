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
       |  "sourceFields": ${createArray(fields.map(StringUtils.quoted): _*)}
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
       |  "stopwords": ${createArray(stopWords.map(StringUtils.quoted): _*)}
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
       | "mappings": ${createArray(mappings.map(StringUtils.quoted): _*)}
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
}
