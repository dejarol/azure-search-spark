package com.github.jarol.azure.search.spark.sql.connector.models

import com.azure.search.documents.models.IndexActionType
import com.github.jarol.azure.search.spark.sql.connector.{DocumentDeserializer, ITDocumentSerializer}

import java.util.{Map => JMap}

/**
 * Bean for read/write integration tests
 * @param id document id
 * @param value value
 */

class BaseActionTypeBean(
                          override val id: String,
                          val value: Option[Int]
                        )
  extends AbstractITDocument(id)

object BaseActionTypeBean {

  implicit object Deserializer extends DocumentDeserializer[BaseActionTypeBean] {
    override def deserialize(document: JMap[String, AnyRef]): BaseActionTypeBean = {
      new BaseActionTypeBean(
        document.getProperty[String]("id"),
        document.maybeGetProperty[Int]("value")
      )
    }
  }
}

/**
 * Bean for read/write integration tests, including a further field hosting a [[IndexActionType]]
 * @param id document id
 * @param value value
 * @param action action
 */

case class ActionTypeBean(
                           override val id: String,
                           override val value: Option[Int],
                           action: String
                         )
  extends BaseActionTypeBean(id, value) {
}

object ActionTypeBean {

  implicit object Serializer
    extends ITDocumentSerializer[ActionTypeBean] {
    override protected def extend(document: ActionTypeBean, map: JMap[String, AnyRef]): JMap[String, AnyRef] = {
      map.maybeAddProperty("value", document.value)
        .addProperty[String]("action", document.action)
    }
  }

  /**
   * Create an instance
   * @param id id
   * @param value value
   * @param action action
   * @return a bean instance
   */

  def apply(
             id: String,
             value: Option[Int],
             action: IndexActionType
           ): ActionTypeBean = {

    ActionTypeBean(
      id,
      value,
      action.name()
    )
  }
}
