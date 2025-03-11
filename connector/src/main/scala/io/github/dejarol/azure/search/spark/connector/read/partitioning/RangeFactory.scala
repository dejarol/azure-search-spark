package io.github.dejarol.azure.search.spark.connector.read.partitioning

import java.lang.{Double => JDouble}
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import scala.util.Try

/**
 * Trait for creating partition values to be then used by
 * <br>
 * Given two bounding values of some type, it will compute the delta between these values
 * and return a range of rough evenly-distributed values within the bounding values
 * @tparam T value type
 * @tparam D delta type
 */

trait RangeFactory[T, D] {

  /**
   * Get the natural ordering for range values
   * @return value's [[Ordering]]
   */

  protected def ordering: Ordering[T]

  /**
   * Get the delta between a lower and upper value
   * @param lower lower value
   * @param upper upper value
   * @return delta between the two values
   */

  protected def getDelta(lower: T, upper: T): D

  /**
   * Get the partition stride (similarly to what happens when reading from JDBC, see [[org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions]])
   * @param delta delta
   * @param numPartitions num partitions used for parallel reading
   * @return partition stride
   */

  protected def getStride(delta: D, numPartitions: Int): D

  /**
   * Add a delta to a range value
   * @param value range value
   * @param delta delta value
   * @return range value plus delta amount
   */

  protected def add(value: T, delta: D): T

  /**
   * Try to convert a string into a range value instance
   * @param value range value as string
   * @return a [[Try]]
   */

  protected def tryFromString(value: String): Try[T]

  /**
   * Convert a range value to a string
   * @param value range value
   * @return range value as string
   */

  protected def asString(value: T): String

  /**
   * Create a range of roughly evenly-distributed values
   * @param lower lower bound
   * @param upper upper bound
   * @param numPartitions num partitions
   * @return a range of values
   */

  final def createRange(lower: T, upper: T, numPartitions: Int): Seq[T] = {

    val delta = getDelta(lower, upper)
    val stride = getStride(delta, math.max(numPartitions - 2, 1))

    var current = lower
    var values = Set(current)
    while (ordering.lt(current, upper)) {
      values = values + current
      current = add(current, stride)
    }

    values.toSeq.sorted(ordering)
  }

  /**
   * Enforce a set of strict assertions on provided values, and then create a range of partition bounds
   * <br>
   * For bounds to be generated, the following conditions must hold
   *  - both the lower and upper bound should be valid string values (i.e. the conversion to value types should not fail)
   *  - lower bound should be less than upper bound
   *  - the number o partitions should be greater than 1
   * @param lower lower bound
   * @param upper upper bound
   * @param numPartitions num partitions
   * @return either a [[Throwable]] if one of the conditions does not hold, or the partition bounds
   */

  final def createPartitionBounds(
                                   lower: String,
                                   upper: String,
                                   numPartitions: Int
                                 ): Either[ConfigException, Seq[String]] = {

    val either = for {
      lb <- toInternalValue(SearchPartitioner.LOWER_BOUND_CONFIG, lower)
      ub <- toInternalValue(SearchPartitioner.UPPER_BOUND_CONFIG, upper)
      _ <- isLowerBoundLessThanUpperBound(lb, ub)
      np <- validateNumPartitions(numPartitions)
    } yield createRange(lb, ub, np)

    either.map {
      values => values.map(asString)
    }
  }

  /**
   * Convert one of the bounds to an internal value
   * @param key configuration key used for retrieving the bound
   * @param value bounding value
   * @return either the conversion exception or the internal bounding value
   */

  private def toInternalValue(key: String, value: String): Either[ConfigException, T] = {

   tryFromString(value)
     .toEither.left.map {
      exception =>
        ConfigException.forIllegalOptionValue(key, value, exception)
    }
  }

  /**
   * Evaluate if lower bound is less or equal than the upper bound
   * @param lower lower
   * @param upper upper
   * @return an empty [[Right]] if lower is leq than upper
   */

  private def isLowerBoundLessThanUpperBound(lower: T, upper: T): Either[ConfigException, Unit] = {

    if (ordering.lt(upper, lower)) {
      Left(
        ConfigException.forIllegalOptionValue(
          SearchPartitioner.LOWER_BOUND_CONFIG,
          asString(lower),
          "Lower bound cannot be greater than upper bound"
        )
      )
    } else {
      Right(())
    }
  }

  /**
   * Validate the number of partitions
   * @param numPartitions partition
   * @return an empty [[Right]] if provided number is greater than 1
   */

  private def validateNumPartitions(numPartitions: Int): Either[ConfigException, Int] = {

    if (numPartitions <= 1) {
      Left(
        ConfigException.forIllegalOptionValue(
          SearchPartitioner.NUM_PARTITIONS_CONFIG,
          s"$numPartitions",
          "should be greater than 1"
        )
      )
    } else {
      Right(numPartitions)
    }
  }
}

object RangeFactory {

  case object Int extends RangeFactory[Int, Int] {
    override protected def ordering: Ordering[Int] = Ordering.Int
    override protected def getDelta(lower: Int, upper: Int): Int = upper - lower
    override protected def getStride(delta: Int, numPartitions: Int): Int = delta / numPartitions
    override protected def add(value: Int, delta: Int): Int = value + delta
    override protected def tryFromString(value: String): Try[Int] = {
      Try {
        Integer.parseInt(value)
      }
    }

    override protected def asString(value: Int): String = String.valueOf(value)
  }

  case object Date extends RangeFactory[OffsetDateTime, Long] {
    override protected def ordering: Ordering[OffsetDateTime] = (v1, v2) => v1.compareTo(v2)
    override protected def getDelta(lower: OffsetDateTime, upper: OffsetDateTime): Long = ChronoUnit.SECONDS.between(lower, upper)
    override protected def getStride(delta: Long, numPartitions: Int): Long = delta / numPartitions
    override protected def add(value: OffsetDateTime, delta: Long): OffsetDateTime = value.plus(delta, ChronoUnit.SECONDS)
    override protected def tryFromString(value: String): Try[OffsetDateTime] = Try {
      TimeUtils.offsetDateTimeFromLocalDate(value)
    }

    override protected def asString(value: OffsetDateTime): String = StringUtils.singleQuoted(value.format(Constants.DATETIME_OFFSET_FORMATTER))
  }

  case object Double extends RangeFactory[Double, Double] {
    override protected def ordering: Ordering[Double] = Ordering.Double
    override protected def getDelta(lower: Double, upper: Double): Double = upper - lower
    override protected def getStride(delta: Double, numPartitions: Int): Double = delta / numPartitions
    override protected def add(value: Double, delta: Double): Double = value + delta
    override protected def tryFromString(value: String): Try[Double] = Try {
      JDouble.parseDouble(value)
    }

    override protected def asString(value: Double): String = String.valueOf(value)
  }
}
