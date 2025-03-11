package io.github.dejarol.azure.search.spark.connector.models

import com.azure.search.documents.models.IndexActionType

import java.util.{Map => JMap}

/**
 * Bean for read/write integration tests, including a further field hosting a [[IndexActionType]]
 * @param id document id
 * @param value value
 * @param action action
 */

case class ActionTypeBean(
                           override val id: String,
                           value: Option[Int],
                           action: String
                         )
  extends AbstractITDocument(id) {
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
