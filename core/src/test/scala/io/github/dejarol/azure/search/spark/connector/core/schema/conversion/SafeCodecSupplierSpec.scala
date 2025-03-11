package io.github.dejarol.azure.search.spark.connector.core.schema.conversion

import io.github.dejarol.azure.search.spark.connector.core.{BasicSpec, FieldFactory}
import org.scalatest.EitherValues

/**
 * Mix-in trait for testing subclasses of [[SafeCodecSupplier]]
 */

trait SafeCodecSupplierSpec
  extends BasicSpec
    with FieldFactory
      with EitherValues {

}
