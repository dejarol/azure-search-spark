package com.github.jarol.azure.search.spark.sql.connector.write

import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.SafeMappingSupplier
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output.WriteConverter

/**
 * Mapping supplier for output conversions (i.e. from Spark to Search)
 */

object WriteMappingSupplier
  extends SafeMappingSupplier[WriteConverter](WriteConverterSupplier) {
}
