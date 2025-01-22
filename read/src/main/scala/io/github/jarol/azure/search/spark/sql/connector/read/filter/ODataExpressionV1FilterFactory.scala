package io.github.jarol.azure.search.spark.sql.connector.read.filter

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataType, StructType}

import scala.annotation.tailrec

/**
 * Factory class for creating OData expressions from V1 filters
 * @param schema underlying scan schema (not pruned)
 */

class ODataExpressionV1FilterFactory(private val schema: StructType)
  extends ODataExpressionFactory[Filter] {

  /**
   * Safely creates an OData expression from a V1 filter
   * @param expression filter
   * @return a non-empty OData expression if the given filter is supported
   */

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

  /**
   * Splits an attribute name into segments
   * <br>
   * If the attribute refers to a top-level field, the output collection will contain only one element.
   * Otherwise, if it's a nested field  it will contain more elements (e.g. given a field represented by
   * a <code>parent.child</code> attribute, the output collection will contain 2 elements, <code>parent</code>
   * and <code>child</code>)
   * @param attribute attribute name
   * @return the attribute name divided into segments
   */

  private def getFieldPathOfAttribute(attribute: String): Seq[String] = attribute.split("\\.")

  /**
   * Builds an expression representing a field reference
   * @param attribute field attribute
   * @return an expression representing a field reference
   */

  private def buildReferenceFor(attribute: String): ODataExpression = {

    ODataExpressions.fieldReference(
      getFieldPathOfAttribute(attribute)
    )
  }

  /**
   * Gets a field datatype from a schema
   * <br>
   * For a top-level field, references should contain only the field name, while for a nested field ,
   * it should contain all parents field names and the field name as well (e.g. for a <code>parent.child</code> field,
   * it should be a 2-element collection with both <code>parent</code>, <code>child</code>)
   * @param references field references
   * @param localSchema local schema to be used for retrieving field datatype
   * @throws IllegalStateException if, for a nested field, the parent is not a [[StructType]]
   * @return the field data type
   */

  @tailrec
  @throws[IllegalStateException]
  private def getDatatypeFor(references: Seq[String], localSchema: StructType): DataType = {

    // If it's a top level field, just get the type
    if (references.size.equals(1)) {
      localSchema(references.head).dataType
    } else {
      // If it's a nested field, locate the parent first, ensuring that it's a StructType
      val (head, tail) = (references.head, references.tail)
      val parentFieldDataType = localSchema(head).dataType

      // If the parent is a StructType, detect the dataType by applying recursion
      parentFieldDataType match {
        case s: StructType => getDatatypeFor(tail, s)
        case _ => throw new IllegalStateException(s"Field $head should be a StructType, found a $parentFieldDataType instead")
      }
    }
  }

  /**
   * Builds a comparison expression (i.e. an expression that compares a field with a constant value)
   * @param attribute attribute (left side)
   * @param literalValue constant value (right side)
   * @param comparator OData comparator
   * @return a non-empty OData expression if the literal type is supported
   */

  private def buildComparisonExpression(
                                         attribute: String,
                                         literalValue: Any,
                                         comparator: ODataComparator
                                       ): Option[ODataExpression] = {

    // Get the attribute data type
    val attributeDataType = getDatatypeFor(
      getFieldPathOfAttribute(attribute), schema
    )

    // If a literal can be defined, create the expression
    ODataExpressions
      .safelyGetLiteral(attributeDataType, literalValue)
      .map {
        literal => ODataExpressions.comparison(
          buildReferenceFor(attribute),
          literal,
          comparator
        )
      }
  }

  /**
   * Creates an <code>in</code> expression
   * @param leftSide left side
   * @param constantValues collection of values for <code>in</code> clause
   * @return an <code>in</code> expression
   */

  private def buildInExpression(
                                 leftSide: String,
                                 constantValues: Seq[Any]
                               ): Option[ODataExpression] = {

    // Get the attribute datatype
    val attributeDataType = getDatatypeFor(
      getFieldPathOfAttribute(leftSide),
      schema
    )

    // Compute OData expressions for all literals
    val maybeLiterals = constantValues.map {
      ODataExpressions.safelyGetLiteral(
        attributeDataType, _)
    }

    // If all literals are supported, create the IN expression
    if (maybeLiterals.forall(_.isDefined)) {

      createInExpression(
        buildReferenceFor(leftSide),
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

    // Set the separator
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

  /**
   * Create an <code>IS_NULL</code> or <code>IS_NOT_NULL</code> expression
   * @param attribute left side
   * @param negate true for generating <code>IS_NOT_NULL</code> expressions
   * @return a null equality expression
   */

  private def buildIsNullExpression(
                                     attribute: String,
                                     negate: Boolean
                                   ): Option[ODataExpression] = {

    Some(
      ODataExpressions.isNull(
        buildReferenceFor(attribute),
        negate
      )
    )
  }

  /**
   * Create a logical (<code>and</code> or <code>or</code>) expression
   * @param left left side
   * @param right right side
   * @return a logical expression
   */

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

  /**
   * Create a <code>not</code> expression
   * @param child expression to negate
   * @return a <code>not</code> expression
   */

  private def buildNotExpression(child: Option[ODataExpression]): Option[ODataExpression] = {

    child.map(ODataExpressions.not)
  }
}
