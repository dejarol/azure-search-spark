package com.github.jarol.azure.search.spark.sql.connector.models

import com.github.jarol.azure.search.spark.sql.connector.ITDocumentSerializer

import java.sql.Timestamp
import java.util.{Map => JMap}

case class PushdownBean(
                         override val id: String,
                         value: Option[Int],
                         date: Option[Timestamp]
                       )
  extends AbstractITDocument(id)

object PushdownBean {

  implicit object Serializer extends ITDocumentSerializer[PushdownBean] {

    override protected def extend(
                                   document: PushdownBean,
                                   map: JMap[String, AnyRef]
                                 ): JMap[String, AnyRef] = {

      map.maybeAddProperty("value", document.value)
      map.maybeAddProperty("date", document.date)
    }
  }
}
