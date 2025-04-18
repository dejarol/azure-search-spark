package io.github.dejarol.azure.search.spark.connector.read.partitioning

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters
import io.github.dejarol.azure.search.spark.connector.core.config.{ConfigException, SearchConfig}
import io.github.dejarol.azure.search.spark.connector.core.schema.{SearchFieldFeature, toSearchFieldOperations, toSearchTypeOperations}
import io.github.dejarol.azure.search.spark.connector.read.config.ReadConfig

import java.util.{List => JList}

/**
 * Range partitioner
 * <br>
 * Given a field <b>f1</b> that is filterable, a number of partitions <b>n</b>, a lower bound value and an upper bound value for such field,
 * it will generate <b>n</b> partitions according to the following behavior
 *  - one partition will read documents whose value for <b>f1</b> are null or lower than the lower bound
 *  - one partition will read document whose value for <b>f1</b> is greater or equal than the upper bound
 *  - <b>n - 2</b> partitions will retrieve documents whose values for <b>f1</b> is greater or equal than the lower bound and less than the upper bound,
 *  using <b>n - 2</b> uniformly distributed range of values
 *
 *  Suitable for cases where there exists a generally-speaking equally distributed field
 * @param readConfig read configuration
 */

case class RangePartitioner(override protected val readConfig: ReadConfig)
  extends AbstractSearchPartitioner(readConfig) {

  @throws[ConfigException]
  override def createPartitions(): JList[SearchPartition] = {

    val partitionFieldName = partitionerOptions.unsafelyGet(SearchPartitioner.PARTITION_FIELD_CONFIG, Some(ReadConfig.PARTITIONER_OPTIONS_PREFIX), None)

    // Get either a ConfigException reporting illegal configurations, or the collection of range value
    val either: Either[ConfigException, Seq[String]] = for {
      searchField <- RangePartitioner.getPartitionField(readConfig.getSearchIndexFields, partitionFieldName)
      partitions <- RangePartitioner.getPartitionRangeValues(searchField, partitionerOptions)
    } yield partitions

    either match {
      case Left(configException) => throw configException
      case Right(values) => JavaScalaConverters.seqToList(
        RangePartition.createCollection(
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
   * A [[com.azure.search.documents.indexes.models.SearchField]] is eligible for partitioning if
   *  - it exists
   *  - it's filterable
   *  - its datatype is either numeric (but not single) or date time
   * @param searchFields search fields
   * @param name partition field name
   * @return either a [[io.github.dejarol.azure.search.spark.connector.core.config.ConfigException]] describing the non-eligibility reason, or the field itself
   */

  protected[partitioning] def getPartitionField(
                                                 searchFields: Seq[SearchField],
                                                 name: String
                                               ): Either[ConfigException, SearchField] = {

    val maybeConfigExceptionCauseOrFieldType: Either[IllegalSearchFieldException, SearchField] = searchFields
      .collectFirst {
        // Collect type for namesake field
        case sf if sf.getName.equalsIgnoreCase(name) => sf
      }.toRight(()).left.map {
        // Map non-existing fields to an exception
        _ => IllegalSearchFieldException.nonExisting(name)
      }.right.flatMap(evaluateExistingCandidate)

    // Map the left side to a ConfigException
    maybeConfigExceptionCauseOrFieldType.left.map {
      cause => ConfigException.forIllegalOptionValue(
        SearchPartitioner.PARTITION_FIELD_CONFIG,
        name,
        cause
      )
    }
  }

  /**
   * Evaluate if an existing [[com.azure.search.documents.indexes.models.SearchField]] is a candidate for partitioning
   * <br>
   * * An existing [[com.azure.search.documents.indexes.models.SearchField]] is a valid candidate for partitioning if
   *  - it's filterable
   *  - its datatype is either numeric (but not single) or date time
   * @param searchField Search field
   * @return either a [[IllegalSearchFieldException]] describing the non-eligibility reason, or the field itself
   */

  protected[partitioning] def evaluateExistingCandidate(searchField: SearchField): Either[IllegalPartitioningFieldException, SearchField] = {

    // Evaluate if related Search field is a good candidate
    val sfType = searchField.getType
    val isFilterable = searchField.isEnabledFor(SearchFieldFeature.FILTERABLE)
    val typeIsCandidate = sfType.isCandidateForPartitioning

    if (isFilterable && typeIsCandidate) {
      Right(searchField)
    } else {

      // Set a proper cause
      val cause = if (!isFilterable) {
        IllegalPartitioningFieldException.forNonFilterableField(searchField)
      } else {
        IllegalPartitioningFieldException.forNonPartitionableType(searchField)
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

  private def getPartitionRangeValues(
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
      options.unsafelyGet(SearchPartitioner.LOWER_BOUND_CONFIG, Some(ReadConfig.PARTITIONER_OPTIONS_PREFIX), None),
      options.unsafelyGet(SearchPartitioner.UPPER_BOUND_CONFIG, Some(ReadConfig.PARTITIONER_OPTIONS_PREFIX), None),
      options.unsafelyGetAs(SearchPartitioner.NUM_PARTITIONS_CONFIG, Integer.parseInt, Some(ReadConfig.PARTITIONER_OPTIONS_PREFIX), None)
    )
  }
}
