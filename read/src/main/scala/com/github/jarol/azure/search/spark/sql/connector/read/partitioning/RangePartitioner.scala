package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.core.config.{ConfigException, SearchConfig}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.{SearchFieldFeature, toSearchFieldOperations, toSearchTypeOperations}
import com.github.jarol.azure.search.spark.sql.connector.read.ReadConfig

import java.util

case class RangePartitioner(override protected val readConfig: ReadConfig)
  extends AbstractSearchPartitioner(readConfig) {

  @throws[ConfigException]
  override def createPartitions(): util.List[SearchPartition] = {

    val partitionerOptions = readConfig.partitionerOptions
    val partitionFieldName = partitionerOptions.unsafelyGet(ReadConfig.PARTITION_FIELD_CONFIG)

    // Get either a ConfigException reporting illegal configurations, or the collection of range value
    val either: Either[ConfigException, Seq[String]] = for {
      searchField <- RangePartitioner.getPartitionField(readConfig.getSearchIndexFields, partitionFieldName)
      partitions <- RangePartitioner.getPartitionRangeValues(searchField, partitionerOptions)
    } yield partitions

    either match {
      case Left(configException) => throw configException
      case Right(values) => JavaScalaConverters.seqToList(
        RangePartition.createCollection(
          readConfig.filter,
          readConfig.select,
          partitionFieldName,
          values
        )
      )
    }
  }
}

object RangePartitioner {

  /**
   * Return the eligible partitioning field, if it exists
   * <br>
   * A [[SearchField]] is eligible for partitioning if
   *  - it exists
   *  - it's filterable
   *  - its datatype is either numeric (but not single) or date time
   * @param searchFields search fields
   * @param name partition field name
   * @return either a [[ConfigException]] describing the non-eligibility reason, or the field itself
   */

  protected[partitioning] def getPartitionField(
                                                 searchFields: Seq[SearchField],
                                                 name: String
                                               ): Either[ConfigException, SearchField] = {

    val maybeConfigExceptionCauseOrFieldType: Either[IllegalSearchFieldException, SearchField] = searchFields
      .collectFirst {
        // Collect type for namesake field
        case sf if sf.getName.equalsIgnoreCase(name) => sf
      }.toRight().left.map {
        // Map non-existing fields to an exception
        _ => IllegalSearchFieldException.nonExisting(name)
      }.right.flatMap(evaluateExistingCandidate)

    // Map the left side to a ConfigException
    maybeConfigExceptionCauseOrFieldType.left.map {
      cause => new ConfigException(
        ReadConfig.PARTITION_FIELD_CONFIG,
        name,
        cause
      )
    }
  }

  /**
   * Evaluate if an existing [[SearchField]] is a candidate for partitioning
   * <br>
   * * An existing [[SearchField]] is a valid candidate for partitioning if
   *  - it's filterable
   *  - its datatype is either numeric (but not single) or date time
   * @param searchField Search field
   * @return either a [[IllegalSearchFieldException]] describing the non-eligibility reason, or the field itself
   */

  protected[partitioning] def evaluateExistingCandidate(searchField: SearchField): Either[IllegalSearchFieldException, SearchField] = {

    // Evaluate if related Search field is a good candidate
    val sfType = searchField.getType
    val isFilterable = searchField.isEnabledFor(SearchFieldFeature.FILTERABLE)
    val typeIsCandidate = sfType.isCandidateForPartitioning

    if (isFilterable && typeIsCandidate) {
      Right(searchField)
    } else {

      // Set a proper cause
      val cause = if (!isFilterable) {
        IllegalSearchFieldException
          .notEnabledFor(
            searchField.getName,
            SearchFieldFeature.FILTERABLE
          )
      } else {
        IllegalSearchFieldException
          .fieldTypeNotEligibleForPartitioning(
            searchField
          )
      }

      Left(cause)
    }
  }

  /**
   *
   * Compute the values to use as range filters
   * <br>
   * Values will be computed al long as some conditions (stated in the documentation of [[RangeFactory.createPartitionBounds]]) hold.
   * Otherwise, a [[ConfigException]] will be retrieved
   * @param partitioningField partitioning Search field
   * @param options partitioner options
   * @return either a [[ConfigException]] or the range values
   */

  protected[partitioning] def getPartitionRangeValues(
                                                       partitioningField: SearchField,
                                                       options: SearchConfig
                                                     ): Either[ConfigException, Seq[String]] = {

    val rangeFactory = partitioningField.getType match {
      case SearchFieldDataType.DATE_TIME_OFFSET => RangeFactory.Date
      case SearchFieldDataType.INT32 | SearchFieldDataType.INT64 => RangeFactory.Int
      case SearchFieldDataType.DOUBLE => RangeFactory.Double
      case _ => throw new IllegalStateException(s"No range factory defined for type ${partitioningField.getType}")
    }

    rangeFactory.createPartitionBounds(
      options.unsafelyGet(ReadConfig.LOWER_BOUND_CONFIG),
      options.unsafelyGet(ReadConfig.UPPER_BOUND_CONFIG),
      options.unsafelyGetAs(ReadConfig.NUM_PARTITIONS_CONFIG, Integer.parseInt)
    )
  }
}
