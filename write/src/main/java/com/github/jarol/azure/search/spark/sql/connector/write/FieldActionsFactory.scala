package com.github.jarol.azure.search.spark.sql.connector.write

import com.github.jarol.azure.search.spark.sql.connector.core.schema.SearchFieldAction

trait FieldActionsFactory {

  def actions: Seq[(String, SearchFieldAction)]
}
