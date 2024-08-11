package com.github.jarol.azure.search.spark.sql.connector.write

import com.github.jarol.azure.search.spark.sql.connector.config.{ConfigException, WriteConfig}
import org.apache.spark.sql.connector.write.{Write, WriteBuilder}
import org.apache.spark.sql.types.{DataTypes, StructType}

/**
 * Write builder for Search services
 * @param writeConfig write configuration
 * @param schema schema of input [[org.apache.spark.sql.DataFrame]] (retrieved by [[org.apache.spark.sql.connector.write.LogicalWriteInfo]])
 */

class SearchWriteBuilder(private val writeConfig: WriteConfig,
                         private val schema: StructType)
  extends WriteBuilder {

  override def build(): Write = {

    writeConfig.actionColumn.flatMap {
      SearchWriteBuilder.evaluateIndexActionColumn(_, schema)
    } match {
      case Some(configException) => throw configException
      case None => new SearchWrite(
        writeConfig,
        schema
      )
    }
  }
}

object SearchWriteBuilder {

  /**
   * Evaluate if given column name represent a valid column that could be used for retrieving a per-document action type.
   * A valid column name must refer to an existing string column
   * @param name column name
   * @param schema schema of input [[org.apache.spark.sql.DataFrame]] to data source
   * @return a non-empty option with a [[ConfigException]] if the column name if non-valid
   */

  protected[write] def evaluateIndexActionColumn(name: String, schema: StructType): Option[ConfigException] = {

    // Evaluate validity of given column name
    val existsStringFieldWithSameName: Boolean = schema.exists {
      sf => sf.name.equalsIgnoreCase(name) &&
        sf.dataType.equals(DataTypes.StringType)
    }

    if (existsStringFieldWithSameName) {
      None
    } else {

      // Setup a suitable error message
      val message = if (!schema.exists {
        sf => sf.name.equalsIgnoreCase(name)
      }) {
        s"Column $name does not exist"
      } else {
        s"Column $name is not a string column. Please provide the name of a string column for the action type"
      }

      Some(
        new ConfigException(
          WriteConfig.ACTION_COLUMN_CONFIG,
          name,
          message
        )
      )
    }
  }
}
