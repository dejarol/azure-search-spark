package com.github.jarol.azure.search.spark.sql.connector.models

/**
 * Bean representing a Search GeoPoint
 * @param `type` type
 * @param coordinates coordinates
 */

case class GeoPointBean(
                         `type`: String,
                         coordinates: Seq[Double]
                       )

object GeoPointBean {

  /**
   * Create an instance
   * @param coordinates coordinates
   * @return a GeoPoint instance
   */

  def apply(
           coordinates: Seq[Double]
           ): GeoPointBean = {

    GeoPointBean(
      "Point",
      coordinates
    )
  }
}
