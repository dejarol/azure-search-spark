package com.github.jarol.azure.search.spark.sql.connector.models

import com.github.jarol.azure.search.spark.sql.connector.ITDocumentSerializer

import java.sql.Timestamp
import java.time.Instant
import java.util.{UUID, Map => JMap}

case class PushdownBean(
                         override val id: String,
                         stringValue: Option[String],
                         intValue: Option[Int],
                         dateValue: Option[Timestamp]
                       )
  extends AbstractITDocument(id)

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

  def apply(
             stringValue: Option[String],
             intValue: Option[Int],
             dateValue: Option[Instant]
           ): PushdownBean = {

    PushdownBean(
      UUID.randomUUID().toString,
      stringValue,
      intValue,
      dateValue.map(Timestamp.from)
    )
  }
}
