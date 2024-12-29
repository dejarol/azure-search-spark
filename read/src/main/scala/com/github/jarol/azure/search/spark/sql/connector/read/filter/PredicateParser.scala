package com.github.jarol.azure.search.spark.sql.connector.read.filter

import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.filter.Predicate

object PredicateParser {

  def compile(predicate: Predicate): Option[String] = {

   predicate.name().toLowerCase match {
     case "is_null" => compileNullPredicate(predicate, notNull = false)
     case "is_not_null" => compileNullPredicate(predicate, notNull = true)
     case _ => None
   }
  }

  private[filter] def compileNullPredicate(
                                            predicate: Predicate,
                                            notNull: Boolean
                                          ): Option[String] = {

    predicate.children().headOption.collect {
      case reference: NamedReference =>
        reference.fieldNames().mkString("/")
    }.map {
      field =>
        val operator = if (notNull) "ne" else "eq"
        f"$field $operator null"
    }
  }
}
