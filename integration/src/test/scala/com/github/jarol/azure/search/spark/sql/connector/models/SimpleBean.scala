package com.github.jarol.azure.search.spark.sql.connector.models

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalTime}

/**
 * Simple bean for read/write integration tests
 * @param id id
 * @param date date
 * @param insertTime insert time
 */

case class SimpleBean(
                       id: String,
                       date: Option[Date],
                       insertTime: Option[Timestamp]
                     )

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
      date.map(d => Timestamp.valueOf(d.atTime(LocalTime.MIDNIGHT)))
    )
  }
}