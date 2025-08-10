package io.github.dejarol.azure.search.spark.connector.write.config

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.indexes.models.{IndexDocumentsBatch, SearchField, SearchIndex}
import com.azure.search.documents.models.IndexActionType
import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters
import io.github.dejarol.azure.search.spark.connector.core.config.{ExtendableConfig, IOConfig, SearchIOConfig}
import io.github.dejarol.azure.search.spark.connector.core.utils.Enums
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.types.StructType

import java.util.{Map => JMap}

/**
 * Write configuration
 * @param options  write options passed to the dataSource
 */

case class WriteConfig(override protected val options: CaseInsensitiveMap[String])
  extends SearchIOConfig(options)
    with ExtendableConfig[WriteConfig] {

  /**
   * Updates this configuration by upserting the given key-value pair
   * @param key   key
   * @param value value
   * @return this configuration, with either a newly added key-value pair or an updated pair
   * @since 0.11.0
   */

  override def withOption(key: String, value: String): WriteConfig = {

    this.copy(
      options = options + (key, value)
    )
  }

  /**
   * Updates this configuration by upserting the given key-value pairs
   * @param options map containing new key-value pairs
   * @return a copy of this configuration, with updated key-value pairs
   * @since 0.11.0
   */

  override def withOptions(options: JMap[String, String]): WriteConfig = {

    this.copy(
      options = this.options ++ CaseInsensitiveMap(
        JavaScalaConverters.javaMapToScala(options)
      )
    )
  }

  /**
   * Extends this configuration by setting the index name.
   * The given value will correspond to the <code>index</code> option in write configurations
   * @param name index name
   * @return this config instance, extended with option <code>index</code>
   * @since 0.11.0
   */

  def withIndexName(name: String): WriteConfig = withOption(IOConfig.INDEX_CONFIG, name)

  /**
   * Index a batch of documents on target index
   *
   * @param documents documents to index
   */

  final def indexDocuments(documents: IndexDocumentsBatch[SearchDocument]): Unit = {

    withSearchClientDo {
      _.indexDocuments(documents)
    }
  }

  /**
   * Get the batch size to be used for writing documents along partitions
   *
   * @return batch size for writing
   */

  final def batchSize: Int = {

    getOrDefaultAs(
      WriteConfig.BATCH_SIZE_CONFIG,
      WriteConfig.DEFAULT_BATCH_SIZE_VALUE,
      Integer.parseInt
    )
  }

  /**
   * Return the (optional) [[com.azure.search.documents.models.IndexActionType]] to use for indexing all documents
   * @return action type for indexing all documents
   */

  final def maybeUserSpecifiedAction: Option[IndexActionType] = {

    getAs(
      WriteConfig.ACTION_CONFIG,
      WriteConfig.valueOfIndexActionType
    )
  }

  /**
   * Return the [[com.azure.search.documents.models.IndexActionType]] defined by the user or a default. It will be used for indexing all documents.
   * If not specified, [[WriteConfig.DEFAULT_ACTION_TYPE]] will be used
   *
   * @return action type for indexing all documents
   */

  final def overallAction: IndexActionType = maybeUserSpecifiedAction.getOrElse(WriteConfig.DEFAULT_ACTION_TYPE)

  /**
   * Return the name of a dataframe column that contains a per-document action type.
   * It must be the name of an existing string column whose values can be mapped to an [[com.azure.search.documents.models.IndexActionType]]
   *
   * @return column name for document action
   */

  final def actionColumn: Option[String] = get(WriteConfig.INDEX_ACTION_COLUMN_CONFIG)

  /**
   * Gets the list of field names to exclude from automatic geo conversion. Suitable for scenarios when a
   * Spark struct column, with columns
   *  - <code>type</code> with dataType <code>String</code>
   *  - <code>coordinates</code> with dataType <code>Array(Double)</code>
   *
   * which is a valid structure for a geo point, should be excluded from automatic geo conversion.
   * For top-level fields, use the field name. For nested fields, use the field path (e.g. <code>address.location</code>)
   * @return the list of field names to exclude from automatic geo conversion
   * @since 0.12.0
   */

  final def excludedFromGeoConversion: Option[Seq[String]] = getAsList(WriteConfig.EXCLUDE_FROM_GEO_CONVERSION_CONFIG)

  /**
   * Returns the options for enriching fields on target index. The options are defined by [[WriteConfig.FIELD_OPTIONS_PREFIX]]
   * @return options for enriching fields
   */

  final def searchFieldCreationOptions: SearchFieldCreationOptions = {

    SearchFieldCreationOptions(
      getAllWithPrefix(WriteConfig.FIELD_OPTIONS_PREFIX),
      actionColumn,
      excludedFromGeoConversion
    )
  }

  /**
   * Retrieves the options for creating a search index (i.e. all configurations that have the prefix defined by
   * [[WriteConfig.INDEX_ATTRIBUTES_PREFIX]])
   * @return index creation options
   */

  def searchIndexCreationOptions: SearchIndexEnrichmentOptions = {

    SearchIndexEnrichmentOptions(
      getAllWithPrefix(WriteConfig.INDEX_ATTRIBUTES_PREFIX)
    )
  }

  /**
   * Delete an index
   * @param name name of the index to delete
   * @since 0.11.0
   */

  final def deleteIndex(name: String): Unit = {

    withSearchIndexClientDo {
      client => client.deleteIndex(
        name
      )
    }
  }

  /**
   * Creates an index, using the provided name and Spark schema.
   * Fields from the Spark will be converted to Azure Search fields and enriched with options prefixed by
   * <code>fieldOptions.</code> while the Search index will be enriched with options prefixed by
   * <code>indexAttributes.</code>
   * @param name index name
   * @param schema Spark schema
   * @return the created index
   * @since 0.11.0
   */

  final def createIndex(name: String, schema: StructType): SearchIndex = {

    val searchFields: Seq[SearchField] = searchFieldCreationOptions.toSearchFields(schema)
    val searchIndex: SearchIndex = new SearchIndex(name)
      .setFields(searchFields: _*)

    val maybeEnrichedIndex: SearchIndex = searchIndexCreationOptions.action.map {
      _.apply(searchIndex)
    }.getOrElse(searchIndex)

    withSearchIndexClientDo {
      client =>
        client.createIndex(
          maybeEnrichedIndex
        )
    }
  }
}

object WriteConfig {

  final val BATCH_SIZE_CONFIG = "batchSize"
  final val DEFAULT_BATCH_SIZE_VALUE = 1000
  final val ACTION_CONFIG = "action"
  final val INDEX_ACTION_COLUMN_CONFIG = "actionColumn"
  final val DEFAULT_ACTION_TYPE: IndexActionType = IndexActionType.MERGE_OR_UPLOAD
  final val FIELD_OPTIONS_PREFIX = "fieldOptions."
  final val INDEX_ATTRIBUTES_PREFIX = "indexAttributes."
  final val EXCLUDE_FROM_GEO_CONVERSION_CONFIG = "excludeFromGeoConversion"

  /**
   * Create an instance from a simple map
   * @param options local options
   * @return a write config
   */

  def apply(options: Map[String, String]): WriteConfig = {

    WriteConfig(
      CaseInsensitiveMap(options)
    )
  }

  /**
   * Retrieve the action type related to a value
   * @param value string value
   * @return the [[IndexActionType]] with same case-insensitive name or inner value
   */

  private[write] def valueOfIndexActionType(value: String): IndexActionType = {

    Enums.unsafeValueOf[IndexActionType](
      value,
      (v, s) => v.name().equalsIgnoreCase(s) ||
        v.toString.equalsIgnoreCase(s)
    )
  }
}