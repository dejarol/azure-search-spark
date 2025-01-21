package io.github.jarol.azure.search.spark.sql.connector.read.filter

import io.github.jarol.azure.search.spark.sql.connector.core.Constants
import io.github.jarol.azure.search.spark.sql.connector.core.utils.{StringUtils, TimeUtils}
import org.apache.spark.sql.types.{DataType, DataTypes}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Collection of concrete implementations and factory methods for [[ODataFilterExpression]]
 * <br>
 * In order to create a [[ODataFilterExpression]] from an [[org.apache.spark.sql.connector.expressions.Expression]],
 * use [[ODataFilterExpressionBuilder]] instead
 */

object ODataExpressions {

  /**
   * A field reference
   * @param names field names (only 1 name for a top-level field, a list of names for nested field)
   */

  private[filter] case class FieldReference(private val names: Seq[String])
    extends ODataFilterExpression {
    override def name(): String = "FIELD_REFERENCE"
    override def toUriLiteral: String = names.mkString("/")
  }

  /**
   * A literal (i.e. constant) value
   * @param dataType data type
   * @param value literal value
   */

  private[filter] case class Literal(
                                      private val dataType: DataType,
                                      private val value: Any
                                    )
    extends ODataFilterExpression {

    override def name(): String = "LITERAL"

    override def toUriLiteral: String = {

      dataType match {
        // Create a single quoted string
        case DataTypes.StringType => StringUtils.singleQuoted(
          StringUtils.fromUTF8String(
            value.asInstanceOf[UTF8String]
          )
        )
        case DataTypes.DateType | DataTypes.TimestampType =>
          val offsetDateTime = if (dataType.equals(DataTypes.DateType)) {
            TimeUtils.offsetDateTimeFromEpochDays(value.asInstanceOf[Integer])
          } else {
            TimeUtils.offsetDateTimeFromEpochMicros(value.asInstanceOf[Long])
          }

          offsetDateTime.format(Constants.DATETIME_OFFSET_FORMATTER)
        case _ => String.valueOf(value)
      }
    }
  }

  /**
   * A null equality filter
   * @param left expression that should be evaluated for null equality
   * @param negate true for creating a filter that evaluates non-null equality
   */

  private[filter] case class IsNull(
                                     private val left: ODataFilterExpression,
                                     private val negate: Boolean
                                   )
    extends ODataFilterExpression {

    override def name(): String = if (negate) "IS_NOT_NULL" else "IS_NULL"

    override def toUriLiteral: String = {

      val operator = if (negate) "ne" else "eq"
      s"${left.toUriLiteral} $operator null"
    }
  }

  /**
   * A comparison expression
   * @param left left expression
   * @param right right expression
   * @param comparator comparison operator
   */

  private[filter] case class Comparison(
                                         private val left: ODataFilterExpression,
                                         private val right: ODataFilterExpression,
                                         private val comparator: ODataComparator
                                       )
    extends ODataFilterExpression {

    override def name(): String = "COMPARISON"

    override def toUriLiteral: String = s"${left.toUriLiteral} ${comparator.oDataValue()} ${right.toUriLiteral}"
  }

  /**
   * A <code>not</code> expression
   * @param child expression to negate
   */

  private[filter] case class Not(private val child: ODataFilterExpression)
    extends ODataFilterExpression {
    override def name(): String = "NOT"
    override def toUriLiteral: String = s"not (${child.toUriLiteral})"
  }

  /**
   * An <code>in</code> expression
   * @param left left side
   * @param inList collection of expressions to match
   */

  private[filter] case class In(
                                 private val left: ODataFilterExpression,
                                 private val inList: Seq[ODataFilterExpression],
                                 private val separator: String
                               )
    extends ODataFilterExpression {

    override def name(): String = "IN"

    override def toUriLiteral: String = {

      val exprList = inList.map {
        expr =>
          StringUtils.removeSuffix(
            StringUtils.removePrefix(expr.toUriLiteral, "'"),
            "'"
          )
      }.mkString(separator)

      s"search.in(" +
        s"${left.toUriLiteral}, " +
        s"${StringUtils.singleQuoted(exprList)}, " +
        s"${StringUtils.singleQuoted(separator)}" +
        s")"
    }
  }

  /**
   * A logical expression (<code>and</code> or <code>or</code>)
   * @param expressions expressions to combine
   * @param isAnd true for creating an <code>and</code> expression
   */

  private[filter] case class Logical(
                                      private val expressions: Seq[ODataFilterExpression],
                                      private val isAnd: Boolean
                                    )
    extends ODataFilterExpression {

    override def name(): String = if (isAnd) "AND" else "OR"

    override def toUriLiteral: String = {

      val operator = if (isAnd) "and" else "or"
      expressions.map {
        exp => s"(${exp.toUriLiteral})"
      }.mkString(s" $operator ")
    }
  }

  /**
   * Create a field reference
   * @param names field names (1 for a top-level field, 2 for a nested field, 3 for a nested field within a nested field, etc ...)
   * @return an expression representing a field reference
   */

  def fieldReference(names: Seq[String]): ODataFilterExpression = FieldReference(names)

  /**
   * Create a literal (i.e. constant) expression
   * @param dataType expression data type
   * @param value literal value
   * @return an expression representing a constant
   */

  def literal(
               dataType: DataType,
               value: Any
             ): ODataFilterExpression = {

    Literal(
      dataType,
      value
    )
  }

  /**
   * Create a <code>isNull</code> or <code>isNotNull</code> equality expression
   * @param left left side
   * @param negate true for creating a <code>isNotNull</code> expression
   * @return an expression for filtering documents using null equality expressions
   */

  def isNull(
              left: ODataFilterExpression,
              negate: Boolean
            ): ODataFilterExpression = {

    IsNull(
      left,
      negate
    )
  }

  /**
   * Create a comparison expression
   * @param left left side
   * @param right right side
   * @param comparator comparator
   * @return an expression for filtering documents by comparing two expressions
   */

  def comparison(
                  left: ODataFilterExpression,
                  right: ODataFilterExpression,
                  comparator: ODataComparator
                ): ODataFilterExpression = {

    Comparison(
      left,
      right,
      comparator
    )
  }

  /**
   * Create an expression that negates another
   * @param child expression to negate
   * @return a negation expression
   */

  def not(child: ODataFilterExpression): ODataFilterExpression = Not(child)

  /**
   * Create an <code>in</code> expression
   * @param left left side
   * @param inList list of values for the <code>in</code> clause
   * @return an expression for filtering documents that match a list of values
   */

  def in(
          left: ODataFilterExpression,
          inList: Seq[ODataFilterExpression],
          separator: String
        ): ODataFilterExpression = {

    In(
      left,
      inList,
      separator
    )
  }
  /**
   * Create a logical expression
   * @param expressions expressions to combine
   * @param isAnd true for creating an <code>and</code> expression
   * @return
   */

  def logical(
               expressions: Seq[ODataFilterExpression],
               isAnd: Boolean
             ): ODataFilterExpression = {

    Logical(expressions, isAnd)
  }
}
