package com.github.jarol.azure.search.spark.sql.connector.models

import com.azure.search.documents.models.IndexActionType

/**
 * Bean for read/write integration tests
 * @param id document id
 * @param value value
 * @param action action
 */

case class ActionTypeBean(
                           override val id: String,
                           value: Option[Int],
                           action: String
                         )
  extends ITDocument(id)

object ActionTypeBean {

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
