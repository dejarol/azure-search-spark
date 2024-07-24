package com.github.jarol.azure.search.spark.sql.connector.config

case class ReadConfig(override protected val options: Map[String, String])
  extends AbstractSearchConfig(options, UsageMode.READ)
