package io.github.jarol.azure.search.spark.sql.connector.read.filter

import io.github.jarol.azure.search.spark.sql.connector.core.utils.StringUtils
import org.apache.spark.sql.types.{DataType, DataTypes}

import java.lang.{Double => JDouble, Long => JLong}
import java.sql.Timestamp

/**
 * Collection of concrete implementations and factory methods for [[ODataExpression]]
 * <br>
 * In order to create a [[ODataExpression]], you must invoke the <code>build</code> method of
 * a concreate implementation of a [[ODataExpressionFactory]]
 */

object ODataExpressions {

  import ODataLiterables._

  /**
   * A field reference
   * @param names field names (only 1 name for a top-level field, a list of names for nested field)
   */

  private[filter] case class FieldReference(private val names: Seq[String])
    extends ODataExpression {
    override def toUriLiteral: String = names.mkString("/")
  }

  /**
   * A literal (i.e. constant) value
   *
   * @param value value to be rendered
   * @tparam T value type. Should have an implicit [[ODataLiterable]] in scope
   */

  private[filter] case class V1Literal[T: ODataLiterable](protected val value: T)
    extends ODataExpression {
    override final def toUriLiteral: String = implicitly[ODataLiterable[T]].toLiteral(value)
  }

  /**
   * A null equality filter
   * @param left expression that should be evaluated for null equality
   * @param negate true for creating a filter that evaluates non-null equality
   */

  private[filter] case class IsNull(
                                     private val left: ODataExpression,
                                     private val negate: Boolean
                                   )
    extends ODataExpression {

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
                                         private val left: ODataExpression,
                                         private val right: ODataExpression,
                                         private val comparator: ODataComparator
                                       )
    extends ODataExpression {

    override def toUriLiteral: String = s"${left.toUriLiteral} ${comparator.oDataValue()} ${right.toUriLiteral}"
  }

  /**
   * A <code>not</code> expression
   * @param child expression to negate
   */

  private[filter] case class Not(private val child: ODataExpression)
    extends ODataExpression {
    override def toUriLiteral: String = s"not (${child.toUriLiteral})"
  }

  /**
   * An <code>in</code> expression
   * @param left left side
   * @param inList collection of expressions to match
   */

  private[filter] case class In(
                                 private val left: ODataExpression,
                                 private val inList: Seq[ODataExpression],
                                 private val separator: String
                               )
    extends ODataExpression {

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
                                      private val expressions: Seq[ODataExpression],
                                      private val isAnd: Boolean
                                    )
    extends ODataExpression {

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

  def fieldReference(names: Seq[String]): ODataExpression = FieldReference(names)

  def maybeLiteral(
                    dataType: DataType,
                    value: Any
                  ): Option[ODataExpression] = {

    (dataType, value) match {
      case (DataTypes.StringType, s: String) => Some(V1Literal[String](s))
      case (DataTypes.IntegerType, i: Integer) => Some(V1Literal[Integer](i)(numericLiterable))
      case (DataTypes.LongType, l: JLong) => Some(V1Literal[JLong](l)(numericLiterable))
      case (DataTypes.DoubleType, l: JDouble) => Some(V1Literal[JDouble](l)(numericLiterable))
      case (DataTypes.TimestampType, t: Timestamp) => Some(V1Literal[Timestamp](t))
      case _ => None
    }
  }

  /**
   * Create a <code>isNull</code> or <code>isNotNull</code> equality expression
   * @param left left side
   * @param negate true for creating a <code>isNotNull</code> expression
   * @return an expression for filtering documents using null equality expressions
   */

  def isNull(
              left: ODataExpression,
              negate: Boolean
            ): ODataExpression = {

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
                  left: ODataExpression,
                  right: ODataExpression,
                  comparator: ODataComparator
                ): ODataExpression = {

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

  def not(child: ODataExpression): ODataExpression = Not(child)

  /**
   * Create an <code>in</code> expression
   * @param left left side
   * @param inList list of values for the <code>in</code> clause
   * @return an expression for filtering documents that match a list of values
   */

  def in(
          left: ODataExpression,
          inList: Seq[ODataExpression],
          separator: String
        ): ODataExpression = {

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
               expressions: Seq[ODataExpression],
               isAnd: Boolean
             ): ODataExpression = {

    Logical(expressions, isAnd)
  }
}
