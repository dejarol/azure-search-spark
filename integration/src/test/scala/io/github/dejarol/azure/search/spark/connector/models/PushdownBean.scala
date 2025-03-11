package io.github.dejarol.azure.search.spark.connector.models

import io.github.dejarol.azure.search.spark.connector.core.Constants
import java.sql.Timestamp
import java.time.{LocalDate, LocalTime}
import java.util.{UUID, Map => JMap}

/**
 * Model to use for integration tests that deal with predicate pushdown
 * @param id document id
 * @param stringValue optional string value
 * @param intValue optional int value
 * @param dateValue optional date value
 */

case class PushdownBean(
                         override val id: String,
                         stringValue: Option[String],
                         intValue: Option[Int],
                         dateValue: Option[Timestamp]
                       )
  extends AbstractITDocument(id) {

  /**
   * Get the date value as a [[LocalDate]]
   * @return date value as a local date
   */

  def dateAsLocalDate: Option[LocalDate] = dateValue.map(_.toLocalDateTime.toLocalDate)
}

object PushdownBean {

  implicit object Serializer
    extends ITDocumentSerializer[PushdownBean] {

    override protected def extend(
                                   document: PushdownBean,
                                   map: JMap[String, AnyRef]
                                 ): JMap[String, AnyRef] = {

      map.maybeAddProperty("stringValue", document.stringValue)
        .maybeAddProperty("intValue", document.intValue)
        .maybeAddProperty("dateValue", document.dateValue)
    }
  }

  /**
   * Create an instance
   * @param stringValue optional string value
   * @param intValue optional int value
   * @param dateValue optional date value
   * @return a [[PushdownBean]] instance
   */

  def apply(
             stringValue: Option[String],
             intValue: Option[Int],
             dateValue: Option[LocalDate]
           ): PushdownBean = {

    PushdownBean(
      UUID.randomUUID().toString,
      stringValue,
      intValue,
      dateValue.map {
        date =>
        Timestamp.from(
          date.atTime(LocalTime.MIDNIGHT)
            .atZone(Constants.UTC_OFFSET)
            .toInstant
        )
      }
    )
  }
}
