package com.github.jarol.azure.search.spark.sql.connector.read.filter

import com.github.jarol.azure.search.spark.sql.connector.core.utils.Enums
import org.apache.spark.sql.connector.expressions.{Expression, GeneralScalarExpression, Literal, NamedReference}

/**
 * Builder for generating OData expression adapters
 */

object ODataExpressionBuilder {

  /**
   * Convert an [[Expression]] to an OData filter, if possible
   * @param expression expression to convert
   * @return an OData filter if the input expression is supported, an empty Option otherwise
   */

  final def build(expression: Expression): Option[ODataExpression] = {

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
            children.map(build),
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

  private[filter] def fromNamedReference(ref: NamedReference): ODataExpression = {

    ODataExpressions.fieldReference(
      ref.fieldNames()
    )
  }

  /**
   * Create a literal OData expression
   * @param literal literal Spark expression
   * @return
   */

  private[filter] def fromLiteral(literal: Literal[_]): ODataExpression = {

    ODataExpressions.literal(
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
                                                  left: Option[ODataExpression],
                                                  notNull: Boolean
                                                ): Option[ODataExpression] = {

    left.map {
      expr => ODataExpressions.isNull(
        expr,
        notNull
      )
    }
  }

  private[filter] def fromComparisonExpression(
                                                exprName: String,
                                                leftSide: Option[ODataExpression],
                                                rightSide: Option[ODataExpression]
                                              ): Option[ODataExpression] = {

    // Match the expression name to an OData comparison operator
    val maybeComparator = Enums.safeValueOf[Comparator](
      exprName,
      (c, s) => c.predicateName().equalsIgnoreCase(s)
    )

    // Create the OData filter
    for {
      left <- leftSide
      right <- rightSide
      op <- maybeComparator
    } yield ODataExpressions.comparison(left, right, op)
  }

  private def fromStartWithOrEndWithExpression(
                                                expression: Option[ODataExpression],
                                                prefixOrSuffix: Option[ODataExpression],
                                                forStartsWith: Boolean
                                              ): Option[ODataExpression] = {

    for {
      exp <- expression
      p <- prefixOrSuffix
    } yield ODataExpressions.startsOrEndsWith(exp, p, forStartsWith)
  }

  private def fromContainsExpression(
                                      expression: Option[ODataExpression],
                                      subString: Option[ODataExpression]
                                    ): Option[ODataExpression] = {
    for {
      exp <- expression
      s <- subString
    } yield ODataExpressions.contains(exp, s)
  }

  private def fromInExpression(
                                expression: Option[ODataExpression],
                                inExpressions: Seq[Option[ODataExpression]]
                              ): Option[ODataExpression] = {

    val allDefined = inExpressions.forall(_.isDefined)
    if (allDefined) {
      expression.map {
        exp =>
          ODataExpressions.in(
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

  private def fromNotExpression(expression: Option[ODataExpression]): Option[ODataExpression] = expression.map(ODataExpressions.not)

  private def fromLogicalExpression(
                                     expressions: Seq[Option[ODataExpression]],
                                     isAnd: Boolean
                                   ): Option[ODataExpression] = {

    val allDefined = expressions.forall(_.isDefined)
    if (allDefined) {
      Some(
        ODataExpressions.logical(
          expressions.collect {
            case Some(value) => value
          },
          isAnd
        )
      )
    } else {
      None
    }
  }
}
