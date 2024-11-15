package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.core.utils.StringUtils
import com.github.jarol.azure.search.spark.sql.connector.{DocumentIDGetter, DocumentSerializer}
import com.github.jarol.azure.search.spark.sql.connector.models._

class FacetNullValuePartitionITSpec
  extends SearchPartitionITSPec {

  private lazy val FACET_FIELD_IS_NULL = "facet field is null"
  private lazy val NOT_MATCHING_OTHER_VALUES = "does not match other facet values"

  private lazy val (indexName, facetField) = ("facet-null-value-index", "value")
  private lazy val (john, jane, matchingId) = ("john", "jane", "1")
  private lazy val valueIsNullOrNotEqualToJohn: PairBean[String] => Boolean = _.value match {
    case Some(value) => !value.equals(john)
    case None => true
  }

  private implicit lazy val serializer: DocumentSerializer[PairBean[String]] = PairBean.serializerFor[String]
  private implicit lazy val idGetter: DocumentIDGetter[PairBean[String]] = idGetterFor()

  override def beforeAll(): Unit = {

    // Clean up and create index
    super.beforeAll()
    createIndexFromSchemaOf[PairBean[String]](indexName)
  }

  /**
   * Create a partition instance
   * @param inputFilter input filter
   * @param facets facets
   * @return a partition instance
   */

  private def createPartition(
                               inputFilter: Option[String],
                               facets: Seq[String]
                             ): SearchPartition = {

    FacetNullValuePartition(
      inputFilter,
      None,
      facetField,
      facets.map(StringUtils.singleQuoted)
    )
  }

  describe(anInstanceOf[FacetNullValuePartition]) {
    describe(SHOULD) {
      describe("retrieve documents") {
        it(s"whose $FACET_FIELD_IS_NULL or $NOT_MATCHING_OTHER_VALUES") {

          val documents: Seq[PairBean[String]] = Seq(
            PairBean("1", None),
            PairBean("2", Some(john)),
            PairBean("3", Some(jane))
          )

          val valueIsNullOrNotEqualToJohn: PairBean[String] => Boolean = _.value match {
            case Some(value) => !value.equals(john)
            case None => true
          }

          assertCountPerPartition(
            documents,
            indexName,
            createPartition(None, Seq(john)),
            valueIsNullOrNotEqualToJohn
          )
        }

        it(s"that match the input filter as well") {

          truncateIndex(indexName)
          val documents: Seq[PairBean[String]] = Seq(
            PairBean(matchingId, None),
            PairBean("2", Some(john)),
            PairBean("3", Some(jane))
          )

          val matchingIdPredicate: PairBean[String] => Boolean = _.id.equals(matchingId)
          val expectedPredicate: PairBean[String] => Boolean = p => valueIsNullOrNotEqualToJohn(p) && matchingIdPredicate(p)
          assertCountPerPartition(
            documents,
            indexName,
            createPartition(Some(s"id eq '$matchingId'"), Seq(john)),
            expectedPredicate
          )
        }
      }
    }
  }
}
