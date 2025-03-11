package io.github.dejarol.azure.search.spark.connector.read.filter

import org.apache.spark.sql.types.{DataType, DataTypes}

import java.lang.{Double => JDouble, Long => JLong}
import java.sql.{Date, Timestamp}

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

  private case class FieldReference(private val names: Seq[String])
    extends ODataExpression {
    override def toUriLiteral: String = names.mkString("/")
  }

  /**
   * A literal (i.e. constant) value
   * @param value value to be rendered
   * @tparam T value type. Should have an implicit [[ODataLiterable]] in scope
   */

  private[filter] case class Literal[T: ODataLiterable](protected val value: T)
    extends ODataExpression {
    private lazy val literable = implicitly[ODataLiterable[T]]
    override final def toUriLiteral: String = literable.toLiteral(value)
  }

  /**
   * A null equality filter
   * @param left expression that should be evaluated for null equality
   * @param negate true for creating a filter that evaluates non-null equality
   */

  private case class IsNull(
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

  private case class Comparison(
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

  private case class Not(private val child: ODataExpression)
    extends ODataExpression {
    override def toUriLiteral: String = s"not (${child.toUriLiteral})"
  }

  /**
   * An <code>in</code> expression
   * @param left left side
   * @param inList collection of expressions to match
   */

  private case class In(
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

  private case class Logical(
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

  /**
   * Safely creates a literal expression, by pattern-matching both data type and value type
   * @param dataType data type
   * @param value literal value
   * @return a non-empty expression if both data type and literal type match a supported case
   */

  def safelyGetLiteral(
                        dataType: DataType,
                        value: Any
                      ): Option[ODataExpression] = {

    (dataType, value) match {
      case (DataTypes.StringType, s: String) => Some(Literal[String](s))
      case (DataTypes.IntegerType, i: Integer) => Some(Literal[Integer](i)(numericLiterable))
      case (DataTypes.LongType, l: JLong) => Some(Literal[JLong](l)(numericLiterable))
      case (DataTypes.DoubleType, l: JDouble) => Some(Literal[JDouble](l)(numericLiterable))
      case (DataTypes.DateType, d: Date) => Some(Literal[Date](d))
      case (DataTypes.TimestampType, t: Timestamp) => Some(Literal[Timestamp](t))
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
