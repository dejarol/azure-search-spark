package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, DataTypeException, FieldFactory}
import org.scalatest.{EitherValues, Inspectors}

class RangePartitionerSpec
  extends BasicSpec
    with FieldFactory
      with EitherValues
        with Inspectors {

  describe(`object`[RangePartitioner]) {
    describe(SHOULD) {
      describe("return an exception for") {
        it("missing partition fields") {

          RangePartitioner.maybePartitionFieldType(
            Seq.empty,
            "first"
          ).left.value shouldBe a[NoSuchSearchFieldException]
        }

        it("partition fields with invalid data type") {

          val fieldName = "first"
          RangePartitioner.maybePartitionFieldType(
            Seq(
              createSearchField(fieldName, SearchFieldDataType.COMPLEX)
            ),
            fieldName
          ).left.value shouldBe a[DataTypeException]
        }
      }

      it("return the partition field type for valid partition fields") {

        val fieldName = "name"
        forAll(
          Seq(
            SearchFieldDataType.INT32,
            SearchFieldDataType.INT64,
            SearchFieldDataType.DOUBLE,
            SearchFieldDataType.SINGLE,
            SearchFieldDataType.DATE_TIME_OFFSET
          )
        ) {
          tp => RangePartitioner
            .maybePartitionFieldType(
              Seq(
                createSearchField(fieldName, tp)
              ),
              fieldName
            ).right.value shouldBe tp
        }
      }
    }
  }
}
