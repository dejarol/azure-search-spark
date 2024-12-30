package com.github.jarol.azure.search.spark.sql.connector.read.filter

import com.github.jarol.azure.search.spark.sql.connector.models.{PairBean, _}

class IsNullAdapterSpec
  extends V2ExpressionAdapterSpec {

  /**
   * Create an adapter instance
   * @param fields fields
   * @param negate true for creating a null equality condition
   * @return an adapter instance
   */

  private def createAdapter(
                             fields: Seq[String],
                             negate: Boolean
                           ): V2ExpressionAdapter = {

    IsNullAdapter(fields, negate)
  }

  /**
   * Assert the correct behavior for the adapter
   * @param documents documents written on test index
   * @param fields set of fields for the adapter
   * @param eqNullPredicate predicate for retrieving documents with null field
   * @tparam T value type for [[PairBean]]
   */

  private def assertAdapterBehavior[T](
                                        documents: Seq[PairBean[T]],
                                        fields: Seq[String],
                                        eqNullPredicate: PairBean[T] => Boolean
                                      ): Unit = {

    // Compute the set of expected documents
    val (expectedNull, expectedNonNull) = documents.partition(eqNullPredicate)

    // Retrieve actual documents using both null and non-null adapter
    val actualNull = readDocumentsUsingV2Adapter(indexName, createAdapter(fields, negate = false))
    val actualNonNull = readDocumentsUsingV2Adapter(indexName, createAdapter(fields, negate = true))

    // Run assertions
    assertSameSizeAndIds(actualNull, expectedNull)
    assertSameSizeAndIds(actualNonNull, expectedNonNull)
  }

  describe(anInstanceOf[IsNullAdapter]) {
    describe(SHOULD) {
      describe("allow to retrieve documents that have a") {
        describe("top-level field") {
          it("equal to (or not) null") {

            val input: Seq[PairBean[String]] = Seq(
              PairBean.empty,
              PairBean("hello"),
              PairBean.empty
            )

            createAndPopulateIndex[PairBean[String]](indexName, input)(PairBean.serializerFor)
            assertAdapterBehavior[String](input, Seq("value"), _.value.isEmpty)
          }
        }

        describe("nested field") {
          it("equal to (or not) null") {

            val input: Seq[PairBean[PairBean[String]]] = Seq(
              PairBean(PairBean("hello")),
              PairBean.empty,
              PairBean(PairBean.empty)
            )

            dropIndexIfExists(indexName, sleep = true)
            createAndPopulateIndex[PairBean[PairBean[String]]](indexName, input)(
              PairBean.serializerFor(
                documentSerializerOf(
                  PairBean.serializerFor[String]
                )
              )
            )

            val predicate: PairBean[PairBean[String]] => Boolean = document => document.value.forall(_.value.isEmpty)
            assertAdapterBehavior[PairBean[String]](input, Seq("value", "value"), predicate)
          }
        }
      }
    }
  }
}
