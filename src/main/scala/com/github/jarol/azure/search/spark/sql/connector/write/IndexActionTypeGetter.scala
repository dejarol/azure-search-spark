package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.models.IndexActionType
import com.github.jarol.azure.search.spark.sql.connector.AzureSparkException
import com.github.jarol.azure.search.spark.sql.connector.utils.Generics
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataTypes, StructType}

/**
 * Getter for retrieving an [[IndexActionType]] from a Spark internal row
 * @param indexOfActionTypeColumn index of the column to use for fetching the action type
 */

case class IndexActionTypeGetter private(private val indexOfActionTypeColumn: Int)
  extends (InternalRow => IndexActionType) {

  override def apply(v1: InternalRow): IndexActionType = {

    if (v1.isNullAt(indexOfActionTypeColumn)) {
      IndexActionType.MERGE_OR_UPLOAD
    } else {
      Generics.safeValueOfEnum[IndexActionType](
        v1.getString(indexOfActionTypeColumn),
        (e, s) => e.name().equalsIgnoreCase(s) || e.toString.equalsIgnoreCase(s)
      ).getOrElse(
        IndexActionType.MERGE_OR_UPLOAD
      )
    }
  }
}

object IndexActionTypeGetter {

  /**
   * Create a getter that will retrieve a per-document index action type from an internal row
   * @param name name of the column that should be used for fetching the action type
   * @param schema row schema
   * @throws AzureSparkException if given column name does not exist within the schema or it's not a string column
   * @return a index action getter
   */

  @throws[AzureSparkException]
  def apply(name: String, schema: StructType): IndexActionTypeGetter = {

    val indexOfActionColumn: Int = schema.zipWithIndex.collectFirst {
      case (field, i) if field.name.equalsIgnoreCase(name) &&
        field.dataType.equals(DataTypes.StringType) => i
    }.getOrElse {
      throw new AzureSparkException(
        s"Action column $name could not be found or it's not a string column"
      )
    }

    IndexActionTypeGetter(indexOfActionColumn)
  }
}