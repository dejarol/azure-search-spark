package com.github.jarol.azure.search.spark.sql.connector.read.filter

import org.apache.spark.sql.connector.expressions.{Expression, GeneralScalarExpression, Literal, NamedReference}

/**
 * Builder for generating OData expression adapters
 */

object V2ExpressionAdapterBuilder {

  /**
   * Convert an [[Expression]] to an OData filter, if possible
   * @param expression expression to convert
   * @return an OData filter if the input expression is supported, an empty Option otherwise
   */

  final def build(expression: Expression): Option[V2ExpressionAdapter] = {

    expression match {
      case literal: Literal[_] => Some(fromLiteral(literal))
      case ref: NamedReference => Some(fromNamedReference(ref))
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
          case "in" => fromInExpression(
            build(children.head),
            children.drop(1).map(build)
          )
          case "not" => fromNotExpression(
            build(children.head)
          )
          case "and" | "or" => fromLogicalExpression(
            build(children.head),
            build(children(1)),
            exprName.equals("and")
          )

          case _ => None
        }
      case _ => None
    }
  }

  /**
   * Convert a named reference to a Search field reference
   * @param ref reference
   * @return a string representing a top-level or nested field
   */

  private[filter] def fromNamedReference(ref: NamedReference): V2ExpressionAdapter = {

    V2ExpressionAdapters.fieldReference(
      ref.fieldNames()
    )
  }

  /**
   * Create a literal OData expression
   * @param literal literal Spark expression
   * @return
   */

  private[filter] def fromLiteral(literal: Literal[_]): V2ExpressionAdapter = {

    V2ExpressionAdapters.literal(
      literal.dataType(),
      literal.value()
    )
  }

  /**
   * Create a null equality expression
   * @param left left side
   * @param notNull true for generating <code>isNotNull</code> expressions
   * @return a null equality expression
   */

  private[filter] def fromNullEqualityExpression(
                                                  left: Option[V2ExpressionAdapter],
                                                  notNull: Boolean
                                                ): Option[V2ExpressionAdapter] = {

    left.map {
      expr => V2ExpressionAdapters.isNull(
        expr,
        notNull
      )
    }
  }

  private[filter] def fromComparisonExpression(
                                                exprName: String,
                                                leftSide: Option[V2ExpressionAdapter],
                                                rightSide: Option[V2ExpressionAdapter]
                                              ): Option[V2ExpressionAdapter] = {

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
      left <- leftSide
      right <- rightSide
      op <- maybeODataOperator
    } yield V2ExpressionAdapters.comparison(op, left, right)
  }

  private def fromStartWithOrEndWithExpression(
                                                expression: Option[V2ExpressionAdapter],
                                                prefixOrSuffix: Option[V2ExpressionAdapter],
                                                forStartsWith: Boolean
                                              ): Option[V2ExpressionAdapter] = {

    for {
      exp <- expression
      p <- prefixOrSuffix
    } yield V2ExpressionAdapters.startsOrEndsWith(exp, p, forStartsWith)
  }

  private def fromContainsExpression(
                                      expression: Option[V2ExpressionAdapter],
                                      subString: Option[V2ExpressionAdapter]
                                    ): Option[V2ExpressionAdapter] = {
    for {
      exp <- expression
      s <- subString
    } yield V2ExpressionAdapters.contains(exp, s)
  }

  private def fromInExpression(
                                expression: Option[V2ExpressionAdapter],
                                inExpressions: Seq[Option[V2ExpressionAdapter]]
                              ): Option[V2ExpressionAdapter] = {

    val allInExpressionsAreDefined = inExpressions.forall(_.isDefined)
    if (allInExpressionsAreDefined) {
      expression.map {
        exp =>
          V2ExpressionAdapters.in(
            exp,
            inExpressions.collect {
              case Some(value) => value
            }
          )
      }
    } else {
      None
    }
  }

  private def fromNotExpression(expression: Option[V2ExpressionAdapter]): Option[V2ExpressionAdapter] = expression.map(V2ExpressionAdapters.not)

  private def fromLogicalExpression(
                                     leftSide: Option[V2ExpressionAdapter],
                                     rightSide: Option[V2ExpressionAdapter],
                                     isAnd: Boolean
                                   ): Option[V2ExpressionAdapter] = {

    for {
      left <- leftSide
      right <- rightSide
    } yield V2ExpressionAdapters.logical(left, right, isAnd)
  }
}
