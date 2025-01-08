package com.github.jarol.azure.search.spark.sql.connector.read.filter

import com.github.jarol.azure.search.spark.sql.connector.core.Constants
import com.github.jarol.azure.search.spark.sql.connector.core.utils.{StringUtils, TimeUtils}
import org.apache.spark.sql.types.{DataType, DataTypes}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Collection of concrete implementations and factory methods for [[ODataExpression]]
 * <br>
 * In order to create a [[ODataExpression]] from an [[org.apache.spark.sql.connector.expressions.Expression]],
 * use [[ODataExpressionBuilder]] instead
 */

object ODataExpressions {

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
   * @param dataType data type
   * @param value literal value
   */

  private case class Literal(
                              private val dataType: DataType,
                              private val value: Any
                            )
    extends ODataExpression {

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
            TimeUtils.fromEpochDays(value.asInstanceOf[Integer])
          } else {
            TimeUtils.fromEpochMicros(value.asInstanceOf[Long])
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
                                 private val comparator: Comparator
                               )
    extends ODataExpression {

    override def toUriLiteral: String = s"${left.toUriLiteral} ${comparator.oDataValue()} ${right.toUriLiteral}"
  }

  /**
   * A <code>startsWith</code> or <code>endsWith</code> expression
   * @param left left expression
   * @param right right expression
   * @param startsWith true for creating a <code>startsWith</code> expression
   */

  private case class StartsOrEndsWith(
                                       private val left: ODataExpression,
                                       private val right: ODataExpression,
                                       private val startsWith: Boolean
                                     )
    extends ODataExpression {

    override def toUriLiteral: String = {

      val oDataFunction = if (startsWith) "startsWith" else "endsWith"
      s"$oDataFunction(${left.toUriLiteral}, ${right.toUriLiteral})"
    }
  }

  /**
   * A <code>contains</code> expression (used for checking that a string field contains a substring)
   * @param left left expression
   * @param substring right expression
   */

  private case class Contains(
                               private val left: ODataExpression,
                               private val substring: ODataExpression
                             )
    extends ODataExpression {

    override def toUriLiteral: String = s"contains(${left.toUriLiteral}, ${substring.toUriLiteral})"
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
                         private val inList: Seq[ODataExpression]
                       )
    extends ODataExpression {

    override def toUriLiteral: String = {

      val inListString = inList.map(_.toUriLiteral).mkString(",")
      s"${left.toUriLiteral} in ($inListString)"
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
   * Create a literal (i.e. constant) expression
   * @param dataType expression data type
   * @param value literal value
   * @return an expression representing a constant
   */

  def literal(
               dataType: DataType,
               value: Any
             ): ODataExpression = {

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
                  comparator: Comparator
                ): ODataExpression = {

    Comparison(
      left,
      right,
      comparator
    )
  }

  /**
   * Create a <code>startsWith</code> or <code>endsWith</code> expression
   * @param left left side
   * @param prefixOrSuffix prefix in case of <code>startsWith</code>, suffix for a <code>endsWith</code>
   * @param starts true for generating a <code>startsWith</code> expression
   * @return an expression for filtering documents based on string start or end
   */

  def startsOrEndsWith(
                        left: ODataExpression,
                        prefixOrSuffix: ODataExpression,
                        starts: Boolean
                      ): ODataExpression = {

    StartsOrEndsWith(
      left,
      prefixOrSuffix,
      starts
    )
  }

  /**
   * Create a <code>contains</code> expression
   * @param left left side
   * @param subString substring
   * @return an expression for filtering documents where a string field contains a substring
   */

  def contains(
                left: ODataExpression,
                subString: ODataExpression
              ): ODataExpression = {

    Contains(
      left,
      subString
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
          inList: Seq[ODataExpression]
        ): ODataExpression = {

    In(
      left,
      inList
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
