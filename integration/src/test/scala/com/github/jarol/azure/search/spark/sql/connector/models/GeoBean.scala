package com.github.jarol.azure.search.spark.sql.connector.models

import com.github.jarol.azure.search.spark.sql.connector.{DocumentDeserializer, DocumentSerializer}

import java.util.{LinkedHashMap => JLMap, Map => JMap}

/**
 * Bean representing a Search GeoPoint
 * @param `type` type
 * @param coordinates coordinates
 */

case class GeoBean(
                    `type`: String,
                    coordinates: Seq[Double]
                  )

object GeoBean {

  /**
   * Document serializer
   */

  implicit object Serializer extends DocumentSerializer[GeoBean] {
    override def serialize(document: GeoBean): JMap[String, AnyRef] = {

      new JLMap[String, AnyRef]()
        .addProperty("type", document.`type`)
        .addArray("coordinates", document.coordinates)
    }
  }

  /**
   * Document deserializer
   */

  implicit object Deserializer extends DocumentDeserializer[GeoBean] {
    override def deserialize(document: JMap[String, AnyRef]): GeoBean = {
      GeoBean(
        document.getProperty("type"),
        document.getArray[Double]("coordinates")
      )
    }
  }

  /**
   * Create an instance
   * @param coordinates coordinates
   * @return a GeoPoint instance
   */

  def apply(
           coordinates: Seq[Double]
           ): GeoBean = {

    GeoBean(
      "Point",
      coordinates
    )
  }
}
