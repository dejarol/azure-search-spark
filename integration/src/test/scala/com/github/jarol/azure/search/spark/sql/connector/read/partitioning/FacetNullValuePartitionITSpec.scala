package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.core.utils.StringUtils
import com.github.jarol.azure.search.spark.sql.connector.models._

import java.time.LocalDate

class FacetNullValuePartitionITSpec
  extends AbstractSearchPartitionITSpec {

  private lazy val (john, jane) = ("john", "jane")
  private lazy val (indexName, facetField) = ("facet-null-value-spec", "stringValue")
  private lazy val stringValueIsNullOrNotEqualToJohn: PushdownBean => Boolean = _.stringValue.forall(v => !v.equals(john))
  private lazy val intValueNotNull: PushdownBean => Boolean = _.intValue.isDefined

  private lazy val documents: Seq[PushdownBean] = Seq(
    PushdownBean(None, Some(1), None),
    PushdownBean(Some(john), Some(1), None),
    PushdownBean(Some(john), None, None),
    PushdownBean(None, Some(2), None),
    PushdownBean(Some(jane), Some(2), None),
    PushdownBean(Some(jane), None, Some(LocalDate.now()))
  )

  override def beforeAll(): Unit = {

    // Clean up and create index
    super.beforeAll()
    createIndexFromSchemaOf[PushdownBean](indexName)
    writeDocuments(indexName, documents)
  }

  /**
   * Create a partition instance
   * @param inputFilter input filter
   * @param facets      facets
   * @return a partition instance
   */

  private def createPartition(
                               inputFilter: Option[String],
                               facets: Seq[String]
                             ): FacetNullValuePartition = {

    FacetNullValuePartition(
      SimpleOptionsBuilder.maybeWithFilter(inputFilter),
      facetField,
      facets
    )
  }

  describe(anInstanceOf[FacetNullValuePartition]) {
    describe(SHOULD) {
      it("have partitionId equal to the size of facet values") {

        val values = Seq(1, 2)
        createPartition(
          None,
          values.map(String.valueOf),
        ).getPartitionId shouldBe values.size
      }

      it("create a facet filter that includes null or different values") {

        val facetValues = Seq("v1", "v2")
        val partition = createPartition(None, facetValues)
        val eqNull = s"$facetField eq null"
        val equalToOtherValues = facetValues.map {
          value => s"$facetField eq $value"
        }.mkString(" or ")

        val expected  = s"$eqNull or not ($equalToOtherValues)"
        val actual = partition.facetFilter
        actual.contains(eqNull) shouldBe true
        actual.contains(equalToOtherValues) shouldBe true
        actual shouldBe expected
      }

      describe("retrieve documents that match") {
        it("the partition filter") {

          assertCountPerPartition[PushdownBean](
            documents,
            indexName,
            createPartition(None, Seq(john).map(StringUtils.singleQuoted)),
            stringValueIsNullOrNotEqualToJohn
          )
        }

        it("the partitioner filter and builder predicate") {

          val expectedPredicate: PushdownBean => Boolean = p => stringValueIsNullOrNotEqualToJohn(p) && intValueNotNull(p)
          assertCountPerPartition[PushdownBean](
            documents,
            indexName,
            createPartition(Some("intValue ne null"), Seq(john).map(StringUtils.singleQuoted)),
            expectedPredicate
          )
        }
      }
    }
  }
}
