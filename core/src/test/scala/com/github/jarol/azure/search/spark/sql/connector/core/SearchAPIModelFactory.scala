package com.github.jarol.azure.search.spark.sql.connector.core

import com.github.jarol.azure.search.spark.sql.connector.core.utils.StringUtils

/**
 * Mix-in trait for specs that deal with json representing Azure Search REST API models
 */

trait SearchAPIModelFactory {

  /**
   * Create a json array by joining many json strings
   * @param elements json strings
   * @return a json array
   */

  protected final def createArray(elements: String*): String = elements.mkString("[", ",", "]")

  /**
   * Create a json string representing a [[com.azure.search.documents.indexes.models.SimilarityAlgorithm]]
   * @param name algorithm name
   * @return a json string representing a [[com.azure.search.documents.indexes.models.SimilarityAlgorithm]]
   */

  protected final def createSimpleODataType(name: String): String = {

    s"""
       |{
       | "${TestConstants.ODATA_TYPE}": "$name"
       |}""".stripMargin
  }

  /**
   * Create a json representing a valid instance of [[com.azure.search.documents.indexes.models.BM25SimilarityAlgorithm]]
   * @param k1 k1 value
   * @param b b value
   * @return a json representing a valid instance of [[com.azure.search.documents.indexes.models.BM25SimilarityAlgorithm]]
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
   * Create a json representing a [[com.azure.search.documents.indexes.models.ClassicTokenizer]]
   * @param name name
   * @param maxTokenLength max token length
   * @return a json representing a [[com.azure.search.documents.indexes.models.ClassicTokenizer]]
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
   * Create a json representing a [[com.azure.search.documents.indexes.models.SearchSuggester]]
   * @param name name
   * @param fields fields
   * @return a json representing a [[com.azure.search.documents.indexes.models.SearchSuggester]]
   */

  protected final def createSearchSuggester(
                                             name: String,
                                             fields: Seq[String]
                                           ): String = {
    s"""
       |{
       |  "name": "$name",
       |  "sourceFields": ${fields.map(StringUtils.quoted).mkString("[", ",", "]")}
       |}""".stripMargin
  }

  /**
   * Create a json representing a [[com.azure.search.documents.indexes.models.StopAnalyzer]]
   * @param name name
   * @param stopWords stop words
   * @return a json representing a [[com.azure.search.documents.indexes.models.StopAnalyzer]]
   */

  protected final def createStopAnalyzer(
                                          name: String,
                                          stopWords: Seq[String]
                                        ): String = {

    s"""
       |{
       |  "${TestConstants.ODATA_TYPE}": "#Microsoft.Azure.Search.StopAnalyzer",
       |  "name": "$name",
       |  "stopwords": ${stopWords.map(StringUtils.quoted).mkString("[", ",", "]")}
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
       | "mappings": ${mappings.map(StringUtils.quoted).mkString("[", ",", "]")}
       |}""".stripMargin
  }
}
