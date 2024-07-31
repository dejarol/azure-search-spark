package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.{BasicSpec, JavaScalaConverters, SearchFieldFactory}
import org.scalatest.Inspectors

class FacetedPartitionerSpec
  extends BasicSpec
    with Inspectors
      with SearchFieldFactory {

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

        it("when no initial filter is provided") {

          val partitions: Seq[SearchPartition] = JavaScalaConverters.listToSeq(
            FacetedPartitioner.generatePartitions(
              createField(fieldName, fieldType),
              values,
              emptyFilter,
              None
            )
          )

          forAll(partitions) {
            _ shouldBe a[SearchPartitionImpl]
          }

          val partitionsImpl: Seq[SearchPartitionImpl] = partitions.map {
            _.asInstanceOf[SearchPartitionImpl]
          }

          forAll(partitionsImpl) {
            _.filter shouldBe defined
          }

          partitionsImpl should have size (values.size + 1)

          partitionsImpl.collect {
            case SearchPartitionImpl(Some(filter), _) => filter
          } should contain theSameElementsAs computeExpectedFilters(
            values,
            fieldName,
            emptyFilter,
            FilterValueFormatters.StringFormatter
          )

          forAll(partitionsImpl) {
            _.select shouldBe empty
          }
        }

        it("when also a global filter is provided") {

          val partitions: Seq[SearchPartition] = JavaScalaConverters.listToSeq(
            FacetedPartitioner.generatePartitions(
              createField(fieldName, fieldType),
              values,
              Some(nonEmptyFilter),
              None
            )
          )

          forAll(partitions) {
            _ shouldBe a[SearchPartitionImpl]
          }

          val partitionsImpl: Seq[SearchPartitionImpl] = partitions.map {
            _.asInstanceOf[SearchPartitionImpl]
          }

          forAll(partitionsImpl) {
            _.filter shouldBe defined
          }

          partitionsImpl should have size (values.size + 1)

          partitionsImpl.collect {
            case SearchPartitionImpl(Some(filter), _) => filter
          } should contain theSameElementsAs computeExpectedFilters(
            values,
            fieldName,
            Some(nonEmptyFilter),
            FilterValueFormatters.StringFormatter
          )

          forAll(partitionsImpl) {
            _.select shouldBe empty
          }
        }
      }
    }
  }
}
