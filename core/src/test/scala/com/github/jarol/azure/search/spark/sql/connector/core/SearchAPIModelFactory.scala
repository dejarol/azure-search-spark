package com.github.jarol.azure.search.spark.sql.connector.core

/**
 * Mix-in trait for specs that deal with json representing Azure Search REST API models
 */

trait SearchAPIModelFactory {

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
}
