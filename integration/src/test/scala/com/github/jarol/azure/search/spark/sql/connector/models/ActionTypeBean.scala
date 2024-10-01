package com.github.jarol.azure.search.spark.sql.connector.models

import com.azure.search.documents.models.IndexActionType

case class ActionTypeBean(
                           id: String,
                           value: Option[Int],
                           action: String
                         )

object ActionTypeBean {

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
