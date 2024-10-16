package com.github.jarol.azure.search.spark.sql.connector.models

import com.azure.search.documents.models.IndexActionType

/**
 * Bean for read/write integration tests
 * @param id id
 * @param value value
 * @param action action
 */

case class ActionTypeBean(
                           id: String,
                           value: Option[Int],
                           action: String
                         )

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
