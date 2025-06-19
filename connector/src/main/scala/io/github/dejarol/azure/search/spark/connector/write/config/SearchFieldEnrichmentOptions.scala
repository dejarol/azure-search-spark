package io.github.dejarol.azure.search.spark.connector.write.config

import com.azure.search.documents.indexes.models.SearchField
import io.github.dejarol.azure.search.spark.connector.core.config.SearchConfig
import io.github.dejarol.azure.search.spark.connector.core.schema.{SchemaUtils, SearchFieldAction}
import io.github.dejarol.azure.search.spark.connector.core.utils.json.Json
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
 * @param fieldActions map with keys being field paths and values being an action to apply on the field
 * @param indexActionColumn name of the column to be used for retrieving the index action for batch upload.
 *                          When converting the Spark fields to Search fields, this field will be excluded
 * @since 0.10.0
 */

case class SearchFieldEnrichmentOptions(
                                         private[config] val fieldActions: Map[String, SearchFieldAction],
                                         private[config] val indexActionColumn: Option[String]
                                       ) {

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

  final val DEFAULT_ID_COLUMN = "id"

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

    // Collect potential actions from deserialized attributes
    val fieldNamesAndMaybeAttributes = CaseInsensitiveMap(
      searchConfig.toMap
    ).mapValues {
      Json.safelyReadAsModelUsingJackson[SearchFieldAttributes]
    }

    // Enable the key attribute for the 'id' column
    val fieldNamesAndActions = fieldNamesAndMaybeAttributes.collect {
      case (k, Right(v)) =>
        (k, maybeEnableKeyAttribute(k, v).toAction)
    }.collect {
      case (k, Some(action)) => (k, action)
    }

    SearchFieldEnrichmentOptions(
      fieldNamesAndActions,
      indexActionColumn
    )
  }

  /**
   * Enables the <code>key</code> attribute if the field name is <code>id</code>
   * @param name field name
   * @param searchFieldAttributes set of field attributes
   * @return the input set of attributes with the <code>key</code> attribute enabled if the field name is <code>id</code>,
   *         otherwise the input set of attributes
   * @since 0.10.3
   */

  private[config] def maybeEnableKeyAttribute(
                                               name: String,
                                               searchFieldAttributes: SearchFieldAttributes
                                             ): SearchFieldAttributes = {

    if (name.equalsIgnoreCase(DEFAULT_ID_COLUMN)) {
      searchFieldAttributes.copy(key = Some(true))
    } else searchFieldAttributes
  }
}
