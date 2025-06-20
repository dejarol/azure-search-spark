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
 * @param options write options with prefix <code>fieldOptions.</code>
 * @param indexActionColumn name of the column to be used for retrieving the index action for batch upload.
 *                          When converting the Spark fields to Search fields, this field will be excluded
 * @since 0.10.0
 */

case class SearchFieldCreationOptions(
                                       override protected val options: CaseInsensitiveMap[String],
                                       private[config] val indexActionColumn: Option[String]
                                     )
  extends SearchConfig(options) {

  import SearchFieldCreationOptions._

  // Collect valid field attributes definitions
  private[config] lazy val originalAttributes: Map[String, SearchFieldAttributes] = options.mapValues {
    Json.safelyReadAsModelUsingJackson[SearchFieldAttributes]
  }.collect {
    case (k, Right(v)) => (k, v)
  }

  private[config] lazy val enrichedAttributes: Map[String, SearchFieldAttributes] = enrichAttributes(originalAttributes)
  private[config] lazy val fieldActions: Map[String, SearchFieldAction] = enrichedAttributes.mapValues {
    _.toAction
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

object SearchFieldCreationOptions {

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
           ): SearchFieldCreationOptions = {

    SearchFieldCreationOptions(
      CaseInsensitiveMap[String](searchConfig.toMap),
      indexActionColumn
    )
  }

  /**
   * Process and potentially enrich a set of attributes, according to the following behavior
   *  - if a key field is defined, no enrichment is required
   *  - if no key field is defined, a new key-value pair will be upserted with a key field enabled. That means that,
   *  if a key <code>id</code> is defined in the original attributes, it will be replaced with a new attribute instance
   *  which is a copy of the original, but with the key feature enabled.
   * @param attributes existing attributes
   * @return an enriched set of attributes
   * @since 0.11.0
   */

  private[config] def enrichAttributes(
                                        attributes: Map[String, SearchFieldAttributes]
                                      ): Map[String, SearchFieldAttributes] = {

    // Evaluate if the key field is defined
    val existsKeyField: Boolean = attributes.values.exists {
      _.key.getOrElse(false)
    }

    // If so, no enrichment is required
    if (existsKeyField) {
      attributes
    } else {

      // Otherwise, retrieve the existing attributes for id column, or create a new one
      // and then enable the key feature
      val attributeForIdColumn = attributes.getOrElse(
        DEFAULT_ID_COLUMN,
        SearchFieldAttributes.empty()
      )

      // Upsert the key field
      attributes + (
        DEFAULT_ID_COLUMN ->
          attributeForIdColumn.withKeyFieldEnabled
        )
    }
  }
}