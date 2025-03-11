package io.github.dejarol.azure.search.spark.connector.models

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalTime}
import java.util.{Map => JMap}

/**
 * Simple bean for read/write integration tests
 * @param id document id
 * @param date date
 * @param insertTime insert time
 */

case class SimpleBean(
                       override val id: String,
                       date: Option[Date],
                       insertTime: Option[Timestamp]
                     ) extends AbstractITDocument(id) {
}

object SimpleBean {

  /**
   * Create an instance
   * @param id id
   * @param date date
   * @return a bean instance
   */

  def apply(
             id: String,
             date: Option[LocalDate]
           ): SimpleBean = {

    SimpleBean(
      id,
      date.map(Date.valueOf),
      date.map(d => Timestamp.valueOf(d.atTime(LocalTime.now())))
    )
  }

  implicit object Serializer extends ITDocumentSerializer[SimpleBean] {
    override protected def extend(document: SimpleBean, map: JMap[String, AnyRef]): JMap[String, AnyRef] = {
      map
        .maybeAddProperty("date", document.date)
        .maybeAddProperty("insertTime", document.insertTime)
    }
  }

  implicit object Deserializer extends DocumentDeserializer[SimpleBean] {
    override def deserialize(document: JMap[String, AnyRef]): SimpleBean = {
      SimpleBean(
        document.getProperty[String]("id"),
        document.maybeGetProperty[Date]("date"),
        document.maybeGetProperty[Timestamp]("insertTime")
      )
    }
  }
}