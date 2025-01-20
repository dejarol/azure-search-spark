package io.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.models.IndexActionType
import io.github.jarol.azure.search.spark.sql.connector.core.utils.Enums
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataTypes, StructType}

/**
 * Supplier for retrieving an [[IndexActionType]] from a Spark internal row
 * @param actionColumnIndex index of the column to use for fetching the action type
 * @param default default action type (used when the value of the action column is null or cannot be mapped to an [[IndexActionType]])
 */

case class PerDocumentSupplier private(private val actionColumnIndex: Int,
                                       private val default: IndexActionType)
  extends IndexActionSupplier {

  override def get(row: InternalRow): IndexActionType = {

    if (row.isNullAt(actionColumnIndex)) {
      default
    } else {
      Enums.safeValueOf[IndexActionType](
        row.getString(actionColumnIndex),
        (e, s) => e.name().equalsIgnoreCase(s) || e.toString.equalsIgnoreCase(s)
      ).getOrElse(
        default
      )
    }
  }
}

object PerDocumentSupplier {

  /**
   * Safely create a getter that will retrieve a per-document index action type from an internal row.
   * <br>
   * If the target column does not exist or its datatype is different from a string, a [[Left]] will be retrieved
   * @param name name of the column that should be used for fetching the action type
   * @param schema row schema
   * @param defaultAction default action to use
   * @return either a [[IllegalIndexActionTypeColumnException]] or a supplier instance
   */

  def safeApply(
                 name: String,
                 schema: StructType,
                 defaultAction: IndexActionType
               ): Either[IllegalIndexActionTypeColumnException, PerDocumentSupplier] = {

    // Extract the index of action type column
    val maybeIndexActionColumnIndex: Option[Int] = schema.zipWithIndex.collectFirst {
      case (field, i) if field.name.equalsIgnoreCase(name) &&
        field.dataType.equals(DataTypes.StringType) => i
    }

    // If defined, create the supplier
    maybeIndexActionColumnIndex.toRight(()).left.map {
      _ => new IllegalIndexActionTypeColumnException(name)
    }.right.map {
      PerDocumentSupplier(_, defaultAction)
    }
  }
}