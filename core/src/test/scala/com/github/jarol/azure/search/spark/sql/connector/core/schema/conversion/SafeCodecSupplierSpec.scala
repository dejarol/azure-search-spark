package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion

import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, FieldFactory}
import org.scalatest.EitherValues

/**
 * Mix-in trait for testing subclasses of [[SafeCodecSupplier]]
 */

trait SafeCodecSupplierSpec
  extends BasicSpec
    with FieldFactory
      with EitherValues {

}
