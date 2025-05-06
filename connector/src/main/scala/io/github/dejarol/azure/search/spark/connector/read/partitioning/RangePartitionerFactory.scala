package io.github.dejarol.azure.search.spark.connector.read.partitioning

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import io.github.dejarol.azure.search.spark.connector.core.NoSuchSearchFieldException
import io.github.dejarol.azure.search.spark.connector.core.config.{ConfigException, SearchConfig}
import io.github.dejarol.azure.search.spark.connector.core.schema._
import io.github.dejarol.azure.search.spark.connector.read.config.ReadConfig

/**
 * Factory class for creating a [[RangePartitioner]].
 * In order to successfully create a [[RangePartitioner]], the following partitioner options must be provided
 *  - <code>partitionField</code>: the name of the field to be used for partitioning
 *  - <code>lowerBound</code>: the lower bound of the range
 *  - <code>upperBound</code>: the upper bound of the range
 *  - <code>numPartitions</code>: the number of partitions
 *
 *  Given a field <b>f1</b> that is filterable, a number of partitions <b>n</b>, a lower bound value and an upper bound value for such field,
 *  it will generate <b>n</b> partitions according to the following behavior
 *  - one partition will read documents whose value for <b>f1</b> are null or lower than the lower bound
 *  - one partition will read document whose value for <b>f1</b> is greater or equal than the upper bound
 *  - <b>n - 2</b> partitions will retrieve documents whose values for <b>f1</b> is greater or equal than the lower bound and less than the upper bound,
 *  using <b>n - 2</b> uniformly distributed range of values
 *
 *  Suitable for cases where there exists a generally-speaking equally distributed field
 *
 */

class RangePartitionerFactory
  extends PartitionerFactory {

  import RangePartitionerFactory._

  /**
   * Creates the partitioner instance
   *
   * @param readConfig overall read configuration provided by the user
   * @throws ConfigException if any of the given partitioner options is missing or invalid
   * @return a partitioner instance, to be used for planning input partitions
   */

  @throws[ConfigException]
  override def createPartitioner(readConfig: ReadConfig): SearchPartitioner = {

    // First, retrieve partition field name
    val partitionerOptions = readConfig.partitionerOptions
    val partitionerFieldName = partitionerOptions.unsafelyGet(
      SearchPartitioner.PARTITION_FIELD_CONFIG,
      Some(ReadConfig.PARTITIONER_OPTIONS_PREFIX),
      None
    )

    // Then, evaluate if it's a valid candidate and compute the partition bounds
    val eitherExceptionOrPartitioner: Either[ConfigException, SearchPartitioner] = for {
      searchField <- getPartitionField(readConfig.getSearchIndexFields, partitionerFieldName)
      partitionBounds <- getPartitionBounds(searchField.getType, partitionerOptions)
    } yield RangePartitioner(searchField.getName, partitionBounds)

    eitherExceptionOrPartitioner match {
      case Left(configException) => throw configException
      case Right(partitioner) => partitioner
    }
  }
}

object RangePartitionerFactory {

  /**
   * Return the eligible partitioning field, if it exists
   * <br>
   * A [[com.azure.search.documents.indexes.models.SearchField]] is eligible for partitioning if
   *  - it exists
   *  - it's filterable
   *  - its datatype is either numeric (but not single) or date time
   *
   * @param searchFields search fields
   * @param name         partition field name
   * @return either a [[io.github.dejarol.azure.search.spark.connector.core.config.ConfigException]] describing the non-eligibility reason, or the field itself
   */

  private[partitioning] def getPartitionField(
                                                 searchFields: Seq[SearchField],
                                                 name: String
                                               ): Either[ConfigException, SearchField] = {

    val maybeConfigExceptionCauseOrFieldType: Either[Throwable, SearchField] = searchFields
      .collectFirst {
        // Collect type for namesake field
        case sf if sf.getName.equalsIgnoreCase(name) => sf
      } match {
      case Some(value) => evaluateExistingCandidate(value)
      case None => Left(
        new NoSuchSearchFieldException(name)
      )
    }

    // Map the left side to a ConfigException
    maybeConfigExceptionCauseOrFieldType.left.map {
      cause =>
        ConfigException.forIllegalOptionValue(
          SearchPartitioner.PARTITION_FIELD_CONFIG,
          name,
          cause
        )
    }
  }

  /**
   * Evaluate if an existing Search field is a candidate for partitioning
   * <br>
   * * An existing Search field is a valid candidate for partitioning if
   *  - it's filterable
   *  - its datatype is either numeric (but not single) or date time
   * @param searchField Search field
   * @return either a [[IllegalPartitioningFieldException]] describing the non-eligibility reason, or the field itself
   */

  private[partitioning] def evaluateExistingCandidate(searchField: SearchField): Either[IllegalPartitioningFieldException, SearchField] = {

    // Evaluate if related Search field is a good candidate
    val isFilterable = searchField.isEnabledFor(SearchFieldFeature.FILTERABLE)
    val typeIsCandidate = searchField.getType.isDateTime || searchField.getType.isNumeric

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
   * @param fieldType partitioning field type
   * @param options partitioner options
   * @return either a [[ConfigException]] or the range values
   */

  private def getPartitionBounds(
                                  fieldType: SearchFieldDataType,
                                  options: SearchConfig
                                ): Either[ConfigException, Seq[String]] = {

    val rangeFactory = fieldType match {
      case SearchFieldDataType.DATE_TIME_OFFSET => RangeFactory.Date
      case SearchFieldDataType.INT32 | SearchFieldDataType.INT64 => RangeFactory.Int
      case SearchFieldDataType.DOUBLE => RangeFactory.Double
      case _ => throw new IllegalStateException(s"No range factory defined for type $fieldType")
    }

    // Feed range factory with lower bound, upper bound and num partitions provided by the user
    rangeFactory.createPartitionBounds(
      options.unsafelyGet(SearchPartitioner.LOWER_BOUND_CONFIG, Some(ReadConfig.PARTITIONER_OPTIONS_PREFIX), None),
      options.unsafelyGet(SearchPartitioner.UPPER_BOUND_CONFIG, Some(ReadConfig.PARTITIONER_OPTIONS_PREFIX), None),
      options.unsafelyGetAs(SearchPartitioner.NUM_PARTITIONS_CONFIG, Integer.parseInt, Some(ReadConfig.PARTITIONER_OPTIONS_PREFIX), None)
    )
  }
}
