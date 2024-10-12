package com.github.jarol.azure.search.spark.sql.connector.write

import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{SafeCodecSupplierSpec, SchemaViolationsMixins}

class DecodingSupplierSpec
  extends SafeCodecSupplierSpec
    with SchemaViolationsMixins {

}
