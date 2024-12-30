package com.github.jarol.azure.search.spark.sql.connector.read.filter

import com.github.jarol.azure.search.spark.sql.connector.core.utils.StringUtils
import org.apache.spark.sql.connector.expressions.{Expression, GeneralScalarExpression, Literal, NamedReference}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.unsafe.types.UTF8String

object V2ExpressionODataBuilder {

  final def build(expression: Expression): Option[String] = {

    expression match {
      case literal: Literal[_] => fromLiteral(literal)
      case ref: NamedReference => fromNamedReference(ref)
      case gse: GeneralScalarExpression =>

        val exprName: String = gse.name().toLowerCase
        val children = gse.children()
        exprName match {
          case "is_null" | "is_not_null" => fromNullEqualityExpression(
            build(children.head),
            exprName.contains("not")
          )
          case ">" | ">=" | "=" | "<>" | "<" | "<=" => fromComparisonExpression(
            exprName,
            build(children.head),
            build(children(1))
          )
          case "starts_with" | "ends_with" => fromStartWithOrEndWithExpression(
            build(children.head),
            build(children(1)),
            exprName.startsWith("starts")
          )
          case "contains" => fromContainsExpression(
            build(children.head),
            build(children(1))
          )

          case _ => None
        }
      case _ => None
    }
  }

  private[filter] def fromNamedReference(ref: NamedReference): Option[String] = {

    Some(
      ref.fieldNames().mkString("/")
    )
  }

  private[filter] def fromLiteral(literal: Literal[_]): Option[String] = {

    literal.dataType() match {
      // Create a single quoted string
      case DataTypes.StringType => Some(
        StringUtils.singleQuoted(
          StringUtils.fromUTF8String(
            literal.value().asInstanceOf[UTF8String]
          )
        )
      )
      case _ => Some(String.valueOf(literal.value()))
    }
  }

  private[filter] def fromNullEqualityExpression(
                                                  left: Option[String],
                                                  notNull: Boolean
                                                ): Option[String] = {

    left.map {
      expr =>
        val operator = if (notNull) "ne" else "eq"
        s"$expr $operator null"
    }
  }

  private[filter] def fromComparisonExpression(
                                                exprName: String,
                                                left: Option[String],
                                                right: Option[String]
                                              ): Option[String] = {

    // Match the expression name to an OData comparison operator
    val maybeODataOperator = exprName match {
      case ">" => Some("gt")
      case ">=" => Some("ge")
      case "=" => Some("eq")
      case "<>" => Some("ne")
      case "<" => Some("lt")
      case "<=" => Some("le")
      case _ => None
    }

    // Create the OData filter
    for {
      l <- left
      r <- right
      op <- maybeODataOperator
    } yield s"$l $op $r"
  }

  private[filter] def fromStartWithOrEndWithExpression(
                                                        expression: Option[String],
                                                        prefixOrSuffix: Option[String],
                                                        forStartsWith: Boolean
                                                      ): Option[String] = {

    val oDataFunction = if (forStartsWith) "startsWith" else "endsWith"
    for {
      exp <- expression
      p <- prefixOrSuffix
    } yield s"$oDataFunction($exp, $p)"
  }

  private[filter] def fromContainsExpression(
                                              expression: Option[String],
                                              subString: Option[String]
                                            ): Option[String] = {
    for {
      exp <- expression
      s <- subString
    } yield s"substringof($s, $exp)"
  }
}
