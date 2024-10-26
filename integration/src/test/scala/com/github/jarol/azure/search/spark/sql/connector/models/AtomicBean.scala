package com.github.jarol.azure.search.spark.sql.connector.models

import java.sql.Timestamp
import java.time.OffsetDateTime

/**
 * Bean for read/write integrations tests
 * @param id document id
 * @param intValue int value
 * @param longValue long value
 * @param doubleValue double value
 * @param booleanValue boolean
 * @param timestampValue timestamp
 */

case class AtomicBean(
                       override val id: String,
                       intValue: Option[Int],
                       longValue: Option[Long],
                       doubleValue: Option[Double],
                       booleanValue: Option[Boolean],
                       timestampValue: Option[Timestamp]
                     )
  extends ITDocument(id)

object AtomicBean {

  /**
   * Create an instance
   * @param id id
   * @param intValue int
   * @param longValue long
   * @param doubleValue double
   * @param booleanValue boolean
   * @param timestampValue timestamp
   * @return an instance
   */

  def from(
            id: String,
            intValue: Option[Int],
            longValue: Option[Long],
            doubleValue: Option[Double],
            booleanValue: Option[Boolean],
            timestampValue: Option[OffsetDateTime]
          ): AtomicBean = {

    AtomicBean(
      id,
      intValue,
      longValue,
      doubleValue,
      booleanValue,
      timestampValue.map {
        offsetDateTime => Timestamp.from(
          offsetDateTime.toInstant
        )
      }
    )
  }
}
