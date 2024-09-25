package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.core.config.ConfigException
import com.github.jarol.azure.search.spark.sql.connector.core.utils.Time
import com.github.jarol.azure.search.spark.sql.connector.read.ReadConfig

import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import scala.util.Try

trait RangeAble[T, D] {

  protected def ordering: Ordering[T]

  protected def getDelta(lower: T, upper: T): D

  protected def getStride(delta: D, numPartitions: Int): D

  protected def add(a: T, delta: D): T

  protected def tryFromString(value: String): Try[T]

  final def computeBounds(lower: T, upper: T, numPartitions: Int): Seq[T] = {

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

  final def createPartitionBounds(
                                   lb: String,
                                   ub: String,
                                   numPartitions: Int
                                 ): Either[Throwable, Seq[T]] = {

    for {
      lbInner <- toInternalValue(ReadConfig.LOWER_BOUND_CONFIG, lb)
      ubInner <- toInternalValue(ReadConfig.UPPER_BOUND_CONFIG, ub)
      _ <- isLowerBoundLessThanUpperBound(lbInner, ubInner)
    } yield computeBounds(lbInner, ubInner, numPartitions)
  }

  private def toInternalValue(boundKey: String, boundValue: String): Either[Throwable, T] = {

   tryFromString(boundValue)
     .toEither.left.map {
      exception =>
        new ConfigException(boundKey, boundValue, exception)
    }
  }

  private def isLowerBoundLessThanUpperBound(lower: T, upper: T): Either[Throwable, Unit] = {

    if (ordering.lt(upper, lower)) {
      Left(
        new ConfigException(s"Lower bound ($lower) cannot be greater than upper bound ($upper)")
      )
    } else {
      Right()
    }
  }
}

object RangeAble {

  case object Int extends RangeAble[Int, Int] {
    override protected def ordering: Ordering[Int] = Ordering.Int
    override protected def getDelta(lower: Int, upper: Int): Int = upper - lower
    override protected def getStride(delta: Int, numPartitions: Int): Int = delta / numPartitions
    override protected def add(a: Int, delta: Int): Int = a + delta
    override protected def tryFromString(value: String): Try[Int] = {
      Try {
        Integer.parseInt(value)
      }
    }
  }

  case object Date extends RangeAble[OffsetDateTime, Long] {
    override protected def ordering: Ordering[OffsetDateTime] = (v1, v2) => v1.compareTo(v2)
    override protected def getDelta(lower: OffsetDateTime, upper: OffsetDateTime): Long = ChronoUnit.SECONDS.between(lower, upper)
    override protected def getStride(delta: Long, numPartitions: Int): Long = delta / numPartitions
    override protected def add(a: OffsetDateTime, delta: Long): OffsetDateTime = a.plus(delta, ChronoUnit.SECONDS)
    override protected def tryFromString(value: String): Try[OffsetDateTime] = Time.safelyToOffsetDatetime(value)
  }
}
