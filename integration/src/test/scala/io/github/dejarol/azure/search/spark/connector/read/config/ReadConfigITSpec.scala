package io.github.dejarol.azure.search.spark.connector.read.config

import com.azure.search.documents.SearchDocument
import io.github.dejarol.azure.search.spark.connector.{FieldFactory, SearchITSpec}
import io.github.dejarol.azure.search.spark.connector.core.utils.JavaCollections
import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters
import io.github.dejarol.azure.search.spark.connector.models.PushdownBean
import io.github.dejarol.azure.search.spark.connector.read.partitioning.SearchPartition
import org.scalamock.scalatest.MockFactory
import org.scalatest.OneInstancePerTest

class ReadConfigITSpec
  extends SearchITSpec
    with FieldFactory
      with MockFactory
        with OneInstancePerTest {

  private lazy val indexName = "read-config-it-spec"
  private lazy val (hello, world) = ("hello", "world")

  private lazy val mockPartition = mock[SearchPartition]
  (mockPartition.getPartitionFilter _).expects()
    .returning(s"stringValue eq '$hello'")
    .anyNumberOfTimes()

  private lazy val documents: Seq[PushdownBean] = Seq(
    PushdownBean("one", Some(hello), Some(1), None),
    PushdownBean("two", Some(hello), Some(1), None),
    PushdownBean("three", Some(hello), Some(2), None),
    PushdownBean("four", Some(world), Some(1), None),
    PushdownBean("five", Some(world), Some(2), None),
    PushdownBean("six", Some("john"), None, None),
    PushdownBean("seven", Some("jane"), None, None)
  )

  private lazy val intValueEqTwoFilter = "intValue eq 2"
  private lazy val stringValueEqHelloPredicate: PushdownBean => Boolean = _.stringValue.exists(_.equals(hello))
  private lazy val intValueEqTwoPredicate: PushdownBean => Boolean = _.intValue.exists(_.equals(2))

  override def beforeAll(): Unit = {

    super.beforeAll()
    createIndexFromSchemaOf[PushdownBean](indexName)
    writeDocuments(indexName, documents)
  }

  /**
   * Create a [[ReadConfig]] instance
   * @param filter filter
   * @return a config instance
   */

  private def createConfig(filter: Option[String]): ReadConfig = {

    val minimumOptions = optionsForAuthAndIndex(indexName)
    val otherOptions = Map(
      ReadConfig.SEARCH_OPTIONS_PREFIX + SearchOptionsBuilderImpl.FILTER -> filter
    ).collect {
      case (k, Some(v)) => (k, v)
    }

    ReadConfig(minimumOptions ++ otherOptions)
  }

  /**
   * Assert that the proper set of results is retrieved
   * <br>
   *  - if the filter is defined, we expect the results to be documents that match both the filter and the partition predicate
   *  - if the filter is not defined, we expect the result to be documents that match only partition predicate
   * @param filter filter
   * @param partition partition instance
   * @param expectedPredicate expected predicate for documents
   * @return collection of results containing matched documents
   */

  private def assertCorrectPartitionResults(
                                             filter: Option[String],
                                             partition: SearchPartition,
                                             expectedPredicate: PushdownBean => Boolean
                                           ): Unit = {

    // Compute the set of expected and actual documents
    val expectedDocuments = documents.filter(expectedPredicate)
    val actualDocuments = JavaScalaConverters.listToSeq(
      JavaCollections.iteratorToList(
        createConfig(filter).getResultsForPartition(
          partition
        )
      )
    ).map(_.getDocument(classOf[SearchDocument]))

    // They should have same size and elements
    actualDocuments should have size expectedDocuments.size
    val expectedIds = expectedDocuments.map(_.id)
    val actualIds = actualDocuments.map(_.get("id").asInstanceOf[String])
    actualIds should contain theSameElementsAs expectedIds
  }

  /**
   * Assert that the proper documents count for a partition is retrieved
   * <br>
   *  - if the filter is defined, we expect the results to be documents that match both the filter and the partition predicate
   *  - otherwise, we expect the result to be documents that match only partition predicate
   * @param filter filter
   * @param partition partition
   * @param expectedPredicate expected predicate for counting documents
   */

  private def assertCorrectCountPerPartition(
                                              filter: Option[String],
                                              partition: SearchPartition,
                                              expectedPredicate: PushdownBean => Boolean
                                            ): Unit = {

    createConfig(filter)
      .getCountForPartition(partition) shouldBe documents.count(expectedPredicate)
  }

  /**
   * Asser the correct facet retrieval
   *  - if the filter is defined, we expect to retrieve facets considering only documents that match the filter
   *  - otherwise, we expect to retrieve facets considering all documents
   * @param filter filter for building the [[ReadConfig]]
   * @param facetFieldName facet field name
   * @param predicate expected predicate for matching documents
   * @param facetGetter function for getting the facet
   * @tparam T facet type
   */

  private def assertCorrectFacetsRetrieval[T](
                                               filter: Option[String],
                                               predicate: Option[PushdownBean => Boolean],
                                               facetFieldName: String,
                                               facetGetter: PushdownBean => Option[T]
                                             ): Unit = {

    // Expected documents should be
    // - all documents if the filter is empty
    // - matching documents otherwise
    val matchingDocuments: Seq[PushdownBean] = predicate.map(
      documents.filter
    ).getOrElse(documents)

    // Compute the expected facets
    val expectedFacetResult = matchingDocuments.map {
      doc => (doc.id, facetGetter(doc))
    }.collect {
      case (k, Some(v)) => (k, v)
    }.groupBy {
      case (_, v) => v
    }.mapValues(_.size)

    // Compute the actual facets
    val actualFacetResult = createConfig(filter)
      .getFacets(facetFieldName, facetFieldName)
      .map {
        fr => (
          fr.getAdditionalProperties.get("value").asInstanceOf[T],
          fr.getCount.toInt
        )
      }.toMap

    actualFacetResult should contain theSameElementsAs expectedFacetResult
  }

  describe(anInstanceOf[ReadConfig]) {
    describe(SHOULD) {
      describe("retrieve") {
        it("an iterator of Search results for a partition") {

          assertCorrectPartitionResults(
            None,
            mockPartition,
            stringValueEqHelloPredicate
          )

          assertCorrectPartitionResults(
            Some(intValueEqTwoFilter),
            mockPartition,
            d => stringValueEqHelloPredicate(d) && intValueEqTwoPredicate(d)
          )
        }

        it("the count of documents for a partition") {

          assertCorrectCountPerPartition(
            None,
            mockPartition,
            stringValueEqHelloPredicate
          )

          assertCorrectCountPerPartition(
            Some(intValueEqTwoFilter),
            mockPartition,
            d => stringValueEqHelloPredicate(d) && intValueEqTwoPredicate(d)
          )
        }

        it("the facet results from a facetable field") {

          val facetField = "intValue"
          assertCorrectFacetsRetrieval[Integer](
            None,
            None,
            facetField,
            _.intValue.map(Integer.valueOf)
          )

          assertCorrectFacetsRetrieval[Integer](
            Some(intValueEqTwoFilter),
            Some(intValueEqTwoPredicate),
            facetField,
            _.intValue.map(Integer.valueOf)
          )
        }
      }
    }
  }
}
