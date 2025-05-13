package io.github.dejarol.azure.search.spark.connector.write.config

import com.azure.search.documents.indexes.models.SearchField
import io.github.dejarol.azure.search.spark.connector.core.config.SearchConfig
import io.github.dejarol.azure.search.spark.connector.core.schema.{SchemaUtils, SearchFieldAction}
import io.github.dejarol.azure.search.spark.connector.core.utils.Json
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.types.StructField

/**
 * A set of options to for enriching Search fields prior to index creation.
 * Given a set of Spark fields, an instance of this class will create a set of Search fields according to the
 * following rules:
 *  - if an <code>indexActionColumn</code> is defined, it will be excluded from the schema
 *  - all remaining fields will be converted to Search fields using built-in logic
 *  - if, for a generic field names <code>f1</code>, an action within <code>fieldActions</code> is defined,
 *  it will be applied to this field
 * Actions should include operations like setting <code>facetable</code>, <code>sortable</code>
 * or other field properties defined by the Search API.
 *
 * @param options write configuration options prefixed with <code>fieldOptions.</code>
 * @param indexActionColumn name of the column to be used for retrieving the index action for batch upload.
 *                          When converting the Spark fields to Search fields, this field will be excluded
 */

case class SearchFieldEnrichmentOptions(
                                         override protected val options: CaseInsensitiveMap[String],
                                         private val indexActionColumn: Option[String]
                                       )
  extends SearchConfig(options) {

  // Collect valid and defined options
  private[config] lazy val fieldActions: Map[String, SearchFieldAction] = options.mapValues {
    Json.safelyReadAsModelUsingJackson[SearchFieldAttributes]
  }.collect {
    case (k, Right(v)) => (k, v.toAction)
  }.collect {
    case (k, Some(v)) => (k, v)
  }

  /**
   * Excludes a field from the schema that should be converted to Search fields
   * @param schema schema
   * @return the input schema as is, if no index action column is defined,
   *         otherwise the schema with the index action column excluded
   */

  private[config] def excludeIndexActionColumn(schema: Seq[StructField]): Seq[StructField] = {

    // If an index action column is defined, it should be excluded from the schema
    indexActionColumn match {
      case Some(value) => schema.filterNot {
        _.name.equalsIgnoreCase(value)
      }
      case None => schema
    }
  }

  /**
   * Converts a collection of Spark fields to a collection of Search fields
   * @param sparkFields Spark fields
   * @return a collection of Search fields
   */

  def toSearchFields(sparkFields: Seq[StructField]): Seq[SearchField] = {

    // If an index action column is defined, it should be excluded from the schema
    excludeIndexActionColumn(sparkFields).map {
      field => SchemaUtils.toSearchField(
        field, fieldActions, None
      )
    }
  }
}

object SearchFieldEnrichmentOptions {

  /**
   * Creates an instance by safely parsing all the key-value pairs defined within
   * a [[io.github.dejarol.azure.search.spark.connector.core.config.SearchConfig]] instance to
   * [[io.github.dejarol.azure.search.spark.connector.write.config.SearchFieldAttributes]] and collecting
   * only the defined actions
   * @param indexActionColumn index action column (will be excluded from the schema that should be converted)
   * @param searchConfig a Search config
   * @return a collection of options for creating Search fields
   */

  def apply(
             searchConfig: SearchConfig,
             indexActionColumn: Option[String]
           ): SearchFieldEnrichmentOptions = {

    SearchFieldEnrichmentOptions(
      CaseInsensitiveMap[String](searchConfig.toMap),
      indexActionColumn
    )
  }
}
