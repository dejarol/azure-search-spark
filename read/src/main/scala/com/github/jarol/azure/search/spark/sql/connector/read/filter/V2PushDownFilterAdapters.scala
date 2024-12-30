package com.github.jarol.azure.search.spark.sql.connector.read.filter

import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.filter.Predicate

object V2PushDownFilterAdapters {

  def safeApply(predicate: Predicate): Option[V2PushDownFilterAdapter] = {

   predicate.name().toLowerCase match {
     case "is_null" => nullPredicate(predicate, notNull = false)
     case "is_not_null" => nullPredicate(predicate, notNull = true)
     case _ => None
   }
  }

  private[filter] def nullPredicate(
                                     predicate: Predicate,
                                     notNull: Boolean
                                   ): Option[V2PushDownFilterAdapter] = {

    predicate.children().headOption.collect {
      case reference: NamedReference =>
        IsNullAdapter(reference.fieldNames(), notNull)
    }
  }
}
