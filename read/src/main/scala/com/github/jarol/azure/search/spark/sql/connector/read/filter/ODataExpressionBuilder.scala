package com.github.jarol.azure.search.spark.sql.connector.read.filter

import com.github.jarol.azure.search.spark.sql.connector.core.utils.Enums
import org.apache.spark.sql.connector.expressions.{Expression, GeneralScalarExpression, Literal, NamedReference}

/**
 * Builder for generating OData expressions
 */

object ODataExpressionBuilder {

  /**
   * Convert a Spark [[Expression]] to an OData expression, if possible
   * @param expression expression to convert
   * @return an OData expression if the input expression is supported, an empty Option otherwise
   */

  final def build(expression: Expression): Option[ODataExpression] = {

    expression match {
      case literal: Literal[_] => Some(fromLiteral(literal))
      case ref: NamedReference => Some(fromNamedReference(ref))
      case gse: GeneralScalarExpression =>

        val exprName: String = gse.name().toLowerCase
        val children = gse.children()
        exprName match {
          case "is_null" | "is_not_null" => nullEqualityExpression(build(children.head), exprName.contains("not"))
          case ">" | ">=" | "=" | "<>" | "<" | "<=" => comparisonExpression(
            exprName,
            build(children.head),
            build(children(1))
          )
          case "starts_with" | "ends_with" => startsOrEndWithExpression(
            build(children.head),
            build(children(1)),
            exprName.startsWith("starts")
          )
          case "contains" => containsExpression(build(children.head), build(children(1)))
          case "in" => inExpression(build(children.head), children.drop(1).map(build))
          case "not" => notExpression(build(children.head))
          case "and" | "or" => logicalExpression(children.map(build), exprName.equals("and"))
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

  private[filter] def nullEqualityExpression(
                                                  left: Option[ODataExpression],
                                                  notNull: Boolean
                                                ): Option[ODataExpression] = {

    left.map {
      expr => ODataExpressions.isNull(expr, notNull)
    }
  }

  /**
   * Create a comparison expression
 *
   * @param exprName  expression name (to be resolved against a [[ODataComparator]])
   * @param leftSide  left side
   * @param rightSide right side
   * @return a comparison expression
   */

  private[filter] def comparisonExpression(
                                                exprName: String,
                                                leftSide: Option[ODataExpression],
                                                rightSide: Option[ODataExpression]
                                              ): Option[ODataExpression] = {

    // Match the expression name to an OData comparison operator
    val maybeComparator = Enums.safeValueOf[ODataComparator](
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

  /**
   * Create a <code>startsWith</code> or <code>endsWith</code> expression
   * @param expression expression
   * @param prefixOrSuffix prefix (in case of <code>startsWith</code>) or suffix
   * @param forStartsWith true for creating a <code>startsWith</code> expression
   * @return a <code>startsWith</code> or <code>endsWith</code> expression
   */

  private def startsOrEndWithExpression(
                                         expression: Option[ODataExpression],
                                         prefixOrSuffix: Option[ODataExpression],
                                         forStartsWith: Boolean
                                       ): Option[ODataExpression] = {

    for {
      exp <- expression
      pOrs <- prefixOrSuffix
    } yield ODataExpressions.startsOrEndsWith(exp, pOrs, forStartsWith)
  }

  /**
   * Create a <code>contains</code> expression
   * @param expression expression
   * @param subString substring to be contained within first expression
   * @return a <code>contains</code> expression
   */

  private def containsExpression(
                                  expression: Option[ODataExpression],
                                  subString: Option[ODataExpression]
                                ): Option[ODataExpression] = {
    for {
      exp <- expression
      s <- subString
    } yield ODataExpressions.contains(exp, s)
  }

  /**
   * Create a <code>in</code> expression
   * @param expression expression
   * @param inExpressions expressions to match by <code>in</code> clause
   * @return a <code>in</code> expression
   */

  private def inExpression(
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

  /**
   * Create a <code>not</code> expression
   * @param expression expression to negate
   * @return a <code>not</code> expression
   */

  private def notExpression(expression: Option[ODataExpression]): Option[ODataExpression] = expression.map(ODataExpressions.not)

  /**
   * Create a logical (<code>and</code> or <code>or</code>) expression
   * @param expressions expressions to combine
   * @param createAnd true for creating an <code>and</code> expression
   * @return a logical expression
   */

  private def logicalExpression(
                                 expressions: Seq[Option[ODataExpression]],
                                 createAnd: Boolean
                               ): Option[ODataExpression] = {

    val allDefined = expressions.forall(_.isDefined)
    if (allDefined) {
      Some(
        ODataExpressions.logical(
          expressions.collect {
            case Some(value) => value
          },
          createAnd
        )
      )
    } else {
      None
    }
  }
}
