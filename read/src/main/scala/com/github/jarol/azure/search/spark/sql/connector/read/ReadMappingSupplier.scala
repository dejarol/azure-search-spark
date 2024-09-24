package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.SafeMappingSupplier
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input.ReadConverter

/**
 * Mapping supplier for input conversions (i.e. from Search to Spark)
 */

object ReadMappingSupplier
  extends SafeMappingSupplier[ReadConverter](ReadConverterSupplier)
