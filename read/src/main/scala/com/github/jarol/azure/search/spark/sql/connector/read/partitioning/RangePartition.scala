package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

case class RangePartition(override protected val partitionId: Int,
                          override protected val inputFilter: Option[String],
                          override protected val maybeSelect: Option[Seq[String]],
                          private val fieldName: String,
                          private val lowerBound: Option[String],
                          private val upperBound: Option[String])
  extends SearchPartitionTemplate(partitionId, inputFilter, maybeSelect) {

  override def getSearchFilter: String = {

    val lowerFilter = lowerBound.map {
      v => s"$fieldName ge $v"
    }

    val upperFilter = upperBound.map {
      v => s"$fieldName lt $v"
    }

    combineFilters(
      combineFilters(lowerFilter, upperFilter),
      inputFilter
    ).orNull
  }

  private def combineFilters(f1: Option[String], f2: Option[String]): Option[String] = {

    (f1, f2) match {
      case (Some(v1), Some(v2)) => Some(s"$v1 and $v2")
      case (Some(v1), None) => Some(v1)
      case (None, Some(v2)) => Some(v2)
      case (None, None) => None
    }
  }
}

object RangePartition {

  def createCollection(
                      inputFilter: Option[String],
                      maybeSelect: Option[Seq[String]],
                      fieldName: String,
                      values: Seq[String]
                      ): Seq[RangePartition] = {

    val lowerValues: Seq[Option[String]] = None +: values.map(Some(_))
    val upperValues: Seq[Option[String]] = values.map(Some(_)) :+ None
    lowerValues.zip(upperValues).zipWithIndex.map {
      case ((lb, ub), index) => RangePartition(
        index,
        inputFilter,
        maybeSelect,
        fieldName,
        lb, ub
      )
    }
  }
}
