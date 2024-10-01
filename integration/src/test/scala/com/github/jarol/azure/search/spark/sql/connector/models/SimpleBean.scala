package com.github.jarol.azure.search.spark.sql.connector.models

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalTime}

case class SimpleBean(
                       id: String,
                       date: Option[Date],
                       insertTime: Option[Timestamp]
                     )

object SimpleBean {

  def apply(
           id: String,
           date: Option[LocalDate]
           ): SimpleBean = {

    SimpleBean(
      id,
      date.map(Date.valueOf),
      date.map(d => Timestamp.valueOf(d.atTime(LocalTime.MIDNIGHT)))
    )
  }
}