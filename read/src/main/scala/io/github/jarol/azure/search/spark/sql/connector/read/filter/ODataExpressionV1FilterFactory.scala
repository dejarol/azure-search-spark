package io.github.jarol.azure.search.spark.sql.connector.read.filter

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataType, StructType}

import scala.annotation.tailrec

class ODataExpressionV1FilterFactory(private val schema: StructType)
  extends ODataExpressionFactory[Filter] {

  override def build(expression: Filter): Option[ODataExpression] = {

    expression match {
      case EqualTo(attribute, value) => buildComparisonExpression(attribute, value, ODataComparator.EQ)
      case EqualNullSafe(attribute, value) => buildComparisonExpression(attribute, value, ODataComparator.EQ)
      case GreaterThan(attribute, value) => buildComparisonExpression(attribute, value, ODataComparator.GT)
      case GreaterThanOrEqual(attribute, value) => buildComparisonExpression(attribute, value, ODataComparator.GEQ)
      case LessThan(attribute, value) => buildComparisonExpression(attribute, value, ODataComparator.LT)
      case LessThanOrEqual(attribute, value) => buildComparisonExpression(attribute, value, ODataComparator.LEQ)
      case In(attribute, values) => buildInExpression(attribute, values)
      case IsNull(attribute) => buildIsNullExpression(attribute, negate = false)
      case IsNotNull(attribute) => buildIsNullExpression(attribute, negate = true)
      case And(left, right) => buildLogicalExpression(build(left), build(right), isAnd = true)
      case Or(left, right) => buildLogicalExpression(build(left), build(right), isAnd = false)
      case Not(child) => buildNotExpression(build(child))
      case _ => None
    }
  }

  private def getFieldPathOfAttribute(attribute: String): Seq[String] = attribute.split("\\.")

  private def fieldReferenceOf(attribute: String): ODataExpression = {

    ODataExpressions.fieldReference(
      getFieldPathOfAttribute(attribute)
    )
  }

  @tailrec
  @throws[IllegalStateException]
  private def getDatatypeFor(fieldPath: Seq[String], localSchema: StructType): DataType = {

    if (fieldPath.size.equals(1)) {
      localSchema(fieldPath.head).dataType
    } else {
      val (head, tail) = (fieldPath.head, fieldPath.tail)
      val parentFieldDataType = localSchema(head).dataType
      parentFieldDataType match {
        case s: StructType => getDatatypeFor(tail, s)
        case _ => throw new IllegalStateException(s"Field $head should be a StructType, found a $parentFieldDataType instead")
      }
    }
  }

  private def maybeLiteralFromValue(
                                     attribute: String,
                                     value: Any
                                   ): Option[ODataExpression] = {

    ODataExpressions.maybeLiteral(
      getDatatypeFor(
        getFieldPathOfAttribute(attribute),
        schema
      ),
      value
    )
  }

  private def buildComparisonExpression(
                                         attribute: String,
                                         value: Any,
                                         comparator: ODataComparator
                                       ): Option[ODataExpression] = {

    maybeLiteralFromValue(attribute, value).map {
      literal => ODataExpressions.comparison(
        fieldReferenceOf(attribute),
        literal,
        comparator
      )
    }
  }

  private def buildInExpression(
                                 attribute: String,
                                 values: Seq[Any]
                               ): Option[ODataExpression] = {

    val attributeDataType = getDatatypeFor(
      getFieldPathOfAttribute(attribute),
      schema
    )

    val maybeLiterals = values.map {
      ODataExpressions.maybeLiteral(
        attributeDataType, _)
    }

    if (maybeLiterals.forall(_.isDefined)) {

      createInExpression(
        fieldReferenceOf(attribute),
        maybeLiterals.collect {
          case Some(value) => value
        }
      )
    } else {
      None
    }
  }

  /**
   * Create an <code>in</code> expression
   * @param leftSide left side
   * @param expressions collection of expressions for the <code>in</code> clause
   * @return an <code>in</code> expression
   */

  private def createInExpression(
                                  leftSide: ODataExpression,
                                  expressions: Seq[ODataExpression]
                                ): Option[ODataExpression] = {

    val expressionsAsStrings: Seq[String] = expressions.map(_.toUriLiteral)
    val maybeSeparator: Option[String] = maybeSetSeparator(expressionsAsStrings, ",")
      .orElse(maybeSetSeparator(expressionsAsStrings, ";"))
      .orElse(maybeSetSeparator(expressionsAsStrings, "|"))

    maybeSeparator.map {
      separator =>
        ODataExpressions.in(leftSide, expressions, separator)
    }
  }

  /**
   * Return an optional separator (defined if and only if none of the values contains the separator itself)
   * @param values values
   * @param sep separator
   * @return an optional separator, to use for joining <code>in</code> clause into a single string
   */

  private def maybeSetSeparator(values: Seq[String], sep: String): Option[String] = {

    if (values.exists(_.contains(sep))) {
      None
    } else Some(sep)
  }

  private def buildIsNullExpression(
                                     attribute: String,
                                     negate: Boolean
                                   ): Option[ODataExpression] = {

    Some(
      ODataExpressions.isNull(
        fieldReferenceOf(attribute),
        negate
      )
    )
  }

  private def buildLogicalExpression(
                                      left: Option[ODataExpression],
                                      right: Option[ODataExpression],
                                      isAnd: Boolean
                                    ): Option[ODataExpression] = {

    for {
      l <- left
      r <- right
    } yield ODataExpressions.logical(Seq(l, r), isAnd)
  }

  private def buildNotExpression(child: Option[ODataExpression]): Option[ODataExpression] = {

    child.map(ODataExpressions.not)
  }
}
