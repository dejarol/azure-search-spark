package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig
import com.github.jarol.azure.search.spark.sql.connector.{BasicSpec, FieldFactory, JavaScalaConverters}
import org.scalatest.Inspectors

class FacetedPartitionerSpec
  extends BasicSpec
    with Inspectors
      with FieldFactory {

  /**
   * Compute the expected set of filters wrapped within output partitions
   * @param values facet values
   * @param fieldName field name
   * @param overallFilter overall filter
   * @param formatter formatter
   * @return expected set of filters
   */

  private def computeExpectedFilters(values: Seq[Any],
                                     fieldName: String,
                                     overallFilter: Option[String],
                                     formatter: FilterValueFormatter): Seq[String] = {

    val filterForValues: Seq[String] = values.map {
      v => f"$fieldName eq ${formatter.format(v)}"
    }

    val eqNullOrOtherValuesFilter: String = f"$fieldName eq null or not (${
      filterForValues.mkString(" or ")
    })"

    (filterForValues :+ eqNullOrOtherValuesFilter).map {
      FacetedPartitioner.combineFilters(_, overallFilter)
    }
  }

  /**
   * Create a read config including an optional filter and selection list
   * @param filter filter
   * @param select selection list
   * @return a read config
   */

  private def createReadConfig(filter: Option[String], select: Option[Seq[String]]): ReadConfig = {

    val localOptionsMap = Map(
      ReadConfig.FILTER_CONFIG -> filter,
      ReadConfig.SELECT_CONFIG -> select.map {
        _.mkString(",")
      }
    ).collect {
      case (k, Some(v)) => (k, v)
    }

    ReadConfig(
      localOptionsMap,
      Map.empty
    )
  }

  describe(`object`[FacetedPartitioner]) {
    describe(SHOULD) {
      it("combine two filters into one") {

        val (first, second) = ("first", "second")
        FacetedPartitioner.combineFilters(first, None) shouldBe first
        FacetedPartitioner.combineFilters(first, Some(second)) shouldBe f"$first and $second"
      }

      describe("generate partitions") {

        val values = Seq("hello", "world")
        val (fieldName, fieldType) = ("field", SearchFieldDataType.STRING)
        val emptyFilter: Option[String] = None
        val nonEmptyFilter = "country eq 'ITALY'"
        val selection = Seq("f1", "f2")

        it("when no initial filter is provided") {

          val partitions: Seq[SearchPartition] = JavaScalaConverters.listToSeq(
            FacetedPartitioner.generatePartitions(
              createSearchField(fieldName, fieldType),
              values,
              createReadConfig(emptyFilter, None)
            )
          )

          // Assert size, partitions type, filter and select
          partitions should have size (values.size + 1)

          forAll(partitions) {
            partition =>
              partition shouldBe a[ScalaSearchPartition]
              val impl = partition.asInstanceOf[ScalaSearchPartition]
              impl.maybeFilter shouldBe defined
              impl.maybeSelect shouldBe empty
          }

          // Assert generated filters
          partitions.map {
           _.getFilter
          } should contain theSameElementsAs computeExpectedFilters(
            values,
            fieldName,
            emptyFilter,
            FilterValueFormatters.StringFormatter
          )
        }

        it("when a global filter is provided and a selection is provided") {

          val partitions: Seq[SearchPartition] = JavaScalaConverters.listToSeq(
            FacetedPartitioner.generatePartitions(
              createSearchField(fieldName, fieldType),
              values,
              createReadConfig(Some(nonEmptyFilter), Some(selection))
            )
          )

          // Assert size, partitions type, filter and select
          partitions should have size (values.size + 1)

          forAll(partitions) {
            partition =>
              partition shouldBe a[ScalaSearchPartition]
              val impl = partition.asInstanceOf[ScalaSearchPartition]
              impl.maybeFilter shouldBe defined
              impl.maybeSelect shouldBe Some(selection)
          }

          partitions.map {
            _.getFilter
          } should contain theSameElementsAs computeExpectedFilters(
            values,
            fieldName,
            Some(nonEmptyFilter),
            FilterValueFormatters.StringFormatter
          )
        }
      }
    }
  }
}
