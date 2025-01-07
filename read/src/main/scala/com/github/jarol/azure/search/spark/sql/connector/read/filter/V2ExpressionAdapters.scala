package com.github.jarol.azure.search.spark.sql.connector.read.filter

import com.github.jarol.azure.search.spark.sql.connector.core.Constants
import com.github.jarol.azure.search.spark.sql.connector.core.utils.{StringUtils, TimeUtils}
import org.apache.spark.sql.types.{DataType, DataTypes}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Collection of concrete implementations and factory methods for [[V2ExpressionAdapter]]
 * <br>
 * In order to create a [[V2ExpressionAdapter]] from an [[org.apache.spark.sql.connector.expressions.Expression]],
 * use [[V2ExpressionAdapterFactory]] instead
 */

object V2ExpressionAdapters {

  /**
   * A field reference
   * @param names field names (only 1 name for a top-level field, a list of names for nested field)
   */

  private case class FieldReference(private val names: Seq[String])
    extends V2ExpressionAdapter {

    override def getODataExpression: String = names.mkString("/")
  }

  /**
   * A literal (i.e. constant) value
   * @param dataType data type
   * @param value literal value
   */

  private case class Literal(
                              private val dataType: DataType,
                              private val value: Any
                            ) extends V2ExpressionAdapter {

    override def getODataExpression: String = {

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
                             private val left: V2ExpressionAdapter,
                             private val negate: Boolean
                           )
    extends V2ExpressionAdapter {

    override def getODataExpression: String = {

      val operator = if (negate) "ne" else "eq"
      s"${left.getODataExpression} $operator null"
    }
  }

  /**
   * A comparison expression
   * @param operator comparison operator
   * @param left left expression
   * @param right right expression
   */

  private case class Comparison(
                                 private val operator: String,
                                 private val left: V2ExpressionAdapter,
                                 private val right: V2ExpressionAdapter
                               )
    extends V2ExpressionAdapter {

    override def getODataExpression: String = s"${left.getODataExpression} $operator ${right.getODataExpression}"
  }

  /**
   * A <code>startsWith</code> or <code>endsWith</code> expression
   * @param left left expression
   * @param right right expression
   * @param startsWith true for creating a <code>startsWith</code> expression
   */

  private case class StartsOrEndsWith(
                                       private val left: V2ExpressionAdapter,
                                       private val right: V2ExpressionAdapter,
                                       private val startsWith: Boolean
                                     )
    extends V2ExpressionAdapter {

    override def getODataExpression: String = {

      val oDataFunction = if (startsWith) "startsWith" else "endsWith"
      s"$oDataFunction(${left.getODataExpression}, ${right.getODataExpression})"
    }
  }

  /**
   * A <code>contains</code> expression (used for checking that a string field contains a substring)
   * @param left left expression
   * @param substring right expression
   */

  private case class Contains(
                               private val left: V2ExpressionAdapter,
                               private val substring: V2ExpressionAdapter
                             )
    extends V2ExpressionAdapter {

    override def getODataExpression: String = s"contains(${left.getODataExpression}, ${substring.getODataExpression})"
  }

  /**
   * A <code>not</code> expression
   * @param child expression to negate
   */

  private case class Not(private val child: V2ExpressionAdapter)
    extends V2ExpressionAdapter {
    override def getODataExpression: String = s"not (${child.getODataExpression})"
  }

  /**
   * An <code>in</code> expression
   * @param left left side
   * @param inList collection of expressions to match
   */

  private case class In(
                         private val left: V2ExpressionAdapter,
                         private val inList: Seq[V2ExpressionAdapter]
                       )
    extends V2ExpressionAdapter {

    override def getODataExpression: String = {

      val inListString = inList.map(_.getODataExpression).mkString(",")
      s"${left.getODataExpression} in ($inListString)"
    }
  }

  /**
   * A logical expression (<code>and</code> or <code>or</code>)
   * @param left left side
   * @param right right side
   * @param isAnd true for creating an <code>and</code> expression
   */

  private case class Logical(
                              private val left: V2ExpressionAdapter,
                              private val right: V2ExpressionAdapter,
                              private val isAnd: Boolean
                            )
    extends V2ExpressionAdapter {

    override def getODataExpression: String = {

      val operator = if (isAnd) "and" else "or"
      s"(${left.getODataExpression}) $operator (${right.getODataExpression})"
    }
  }

  /**
   * Create a field reference
   * @param names field names (1 for a top-level field, 2 for a nested field, 3 for a nested field within a nested field, etc ...)
   * @return an expression representing a field reference
   */

  def fieldReference(names: Seq[String]): V2ExpressionAdapter = FieldReference(names)

  /**
   * Create a literal (i.e. constant) expression
   * @param dataType expression data type
   * @param value literal value
   * @return an expression representing a constant
   */

  def literal(
               dataType: DataType,
               value: Any
             ): V2ExpressionAdapter = {

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
              left: V2ExpressionAdapter,
              negate: Boolean
            ): V2ExpressionAdapter = {

    IsNull(
      left,
      negate
    )
  }

  /**
   * Create a comparison expression
   * @param op operator
   * @param left left side
   * @param right right side
   * @return an expression for filtering documents by comparing two expressions
   */

  def comparison(
                  op: String,
                  left: V2ExpressionAdapter,
                  right: V2ExpressionAdapter
                ): V2ExpressionAdapter = {

    Comparison(
      op,
      left,
      right
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
                        left: V2ExpressionAdapter,
                        prefixOrSuffix: V2ExpressionAdapter,
                        starts: Boolean
                      ): V2ExpressionAdapter = {

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
                left: V2ExpressionAdapter,
                subString: V2ExpressionAdapter
              ): V2ExpressionAdapter = {

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

  def not(child: V2ExpressionAdapter): V2ExpressionAdapter = Not(child)

  /**
   * Create an <code>in</code> expression
   * @param left left side
   * @param inList list of values for the <code>in</code> clause
   * @return an expression for filtering documents that match a list of values
   */

  def in(
          left: V2ExpressionAdapter,
          inList: Seq[V2ExpressionAdapter]
        ): V2ExpressionAdapter = {

    In(
      left,
      inList
    )
  }

  /**
   * Create a logical expression
   * @param left left side
   * @param right right side
   * @param isAnd true for creating an <code>and</code> expression
   * @return
   */

  def logical(
               left: V2ExpressionAdapter,
               right: V2ExpressionAdapter,
               isAnd: Boolean
             ): V2ExpressionAdapter = {

    Logical(
      left,
      right,
      isAnd
    )
  }
}
