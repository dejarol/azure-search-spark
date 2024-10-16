package com.github.jarol.azure.search.spark.sql.connector.models

import java.sql.Timestamp
import java.time.LocalDateTime

/**
 * Bean for read/write integrations tests
 * @param id id
 * @param intValue int value
 * @param longValue long value
 * @param doubleValue double value
 * @param floatValue float
 * @param booleanValue boolean
 * @param timestampValue timestamp
 */

case class AtomicBean(
                       id: String,
                       intValue: Option[Int],
                       longValue: Option[Long],
                       doubleValue: Option[Double],
                       floatValue: Option[Float],
                       booleanValue: Option[Boolean],
                       timestampValue: Option[Timestamp]
                     )

object AtomicBean {

  /**
   * Create an instance
   * @param id id
   * @param intValue int
   * @param longValue long
   * @param doubleValue double
   * @param floatValue float
   * @param booleanValue boolean
   * @param timestampValue timestamp
   * @return an instance
   */

  def apply(
             id: String,
             intValue: Option[Int],
             longValue: Option[Long],
             doubleValue: Option[Double],
             floatValue: Option[Float],
             booleanValue: Option[Boolean],
             timestampValue: Option[LocalDateTime]
           ): AtomicBean = {

    AtomicBean(
      id,
      intValue,
      longValue,
      doubleValue,
      floatValue,
      booleanValue,
      timestampValue.map {
        Timestamp.valueOf
      }
    )
  }
}
