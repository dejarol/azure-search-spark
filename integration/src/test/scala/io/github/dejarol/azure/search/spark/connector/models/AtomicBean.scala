package io.github.dejarol.azure.search.spark.connector.models

import java.sql.Timestamp
import java.time.OffsetDateTime
import java.util.{UUID, Map => JMap}

/**
 * Bean for read/write integrations tests
 * @param id document id
 * @param stringValue string value
 * @param intValue int value
 * @param longValue long value
 * @param doubleValue double value
 * @param booleanValue boolean
 * @param timestampValue timestamp
 */

case class AtomicBean(
                       override val id: String,
                       stringValue: Option[String],
                       intValue: Option[Int],
                       longValue: Option[Long],
                       doubleValue: Option[Double],
                       booleanValue: Option[Boolean],
                       timestampValue: Option[Timestamp]
                     )
  extends AbstractITDocument(id) {
}

object AtomicBean {

  implicit object Serializer extends ITDocumentSerializer[AtomicBean] {
    override protected def extend(document: AtomicBean, map: JMap[String, AnyRef]): JMap[String, AnyRef] = {

      map.maybeAddProperty("stringValue", document.stringValue)
        .maybeAddProperty("intValue", document.intValue)
        .maybeAddProperty("longValue", document.longValue)
        .maybeAddProperty("doubleValue", document.doubleValue)
        .maybeAddProperty("booleanValue", document.booleanValue)
        .maybeAddProperty("timestampValue", document.timestampValue)
    }
  }

  /**
   * Create an instance
   * @param stringValue string
   * @param intValue int
   * @param longValue long
   * @param doubleValue double
   * @param booleanValue boolean
   * @param timestampValue timestamp
   * @return an instance
   */

  def apply(
             stringValue: Option[String],
             intValue: Option[Int],
             longValue: Option[Long],
             doubleValue: Option[Double],
             booleanValue: Option[Boolean],
             timestampValue: Option[OffsetDateTime]
           ): AtomicBean = {

    AtomicBean(
      UUID.randomUUID().toString,
      stringValue,
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
