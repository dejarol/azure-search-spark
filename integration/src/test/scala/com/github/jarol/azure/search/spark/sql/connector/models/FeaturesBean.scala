package com.github.jarol.azure.search.spark.sql.connector.models

/**
 * Bean for write integration tests
 * @param id document id
 * @param category category
 * @param level level
 */

case class FeaturesBean(
                         override val id: String,
                         category: Option[String],
                         level: Option[Int]
                       )
  extends AbstractITDocument(id)