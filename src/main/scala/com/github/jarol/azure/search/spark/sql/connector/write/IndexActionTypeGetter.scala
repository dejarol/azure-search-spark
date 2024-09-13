package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.models.IndexActionType
import com.github.jarol.azure.search.spark.sql.connector.utils.Generics
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataTypes, StructType}

/**
 * Getter for retrieving an [[IndexActionType]] from a Spark internal row
 * @param actionColumnIndex index of the column to use for fetching the action type
 * @param default default action type (used when the value of the action column is null or cannot be mapped to an [[IndexActionType]])
 */

case class IndexActionTypeGetter private(private val actionColumnIndex: Int,
                                         private val default: IndexActionType)
  extends (InternalRow => IndexActionType) {

  override def apply(v1: InternalRow): IndexActionType = {

    if (v1.isNullAt(actionColumnIndex)) {
      default
    } else {
      Generics.safeValueOfEnum[IndexActionType](
        v1.getString(actionColumnIndex),
        (e, s) => e.name().equalsIgnoreCase(s) || e.toString.equalsIgnoreCase(s)
      ).getOrElse(
        default
      )
    }
  }
}

object IndexActionTypeGetter {

  /**
   * Create a getter that will retrieve a per-document index action type from an internal row
   * @param name name of the column that should be used for fetching the action type
   * @param schema row schema
   * @param defaultAction default action to use
   * @throws AzureSparkException if given column name does not exist within the schema or it's not a string column
   * @return a index action getter
   */

  @throws[IllegalIndexActionTypeColumnException]
  def apply(name: String, schema: StructType, defaultAction: IndexActionType): IndexActionTypeGetter = {

    val indexOfActionColumn: Int = schema.zipWithIndex.collectFirst {
      case (field, i) if field.name.equalsIgnoreCase(name) &&
        field.dataType.equals(DataTypes.StringType) => i
    }.getOrElse {
      throw new IllegalIndexActionTypeColumnException(name)
    }

    IndexActionTypeGetter(
      indexOfActionColumn,
      defaultAction
    )
  }
}