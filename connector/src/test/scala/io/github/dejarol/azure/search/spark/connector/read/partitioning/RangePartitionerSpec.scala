package io.github.dejarol.azure.search.spark.connector.read.partitioning

import io.github.dejarol.azure.search.spark.connector.BasicSpec
import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters

class RangePartitionerSpec
  extends BasicSpec {

  /**
   * Creates a partitioner instance and retrieves input partitions
   * @param fieldName candidate field name
   * @param partitionBounds partition bounds
   * @return planned partitions
   */

  private def planInputPartitions(
                                   fieldName: String,
                                   partitionBounds: Seq[String]
                                 ): Seq[SearchPartition] = {

    JavaScalaConverters.listToSeq(
      RangePartitioner(
        fieldName,
        partitionBounds
      ).createPartitions()
    )
  }

  describe(anInstanceOf[RangePartitioner]) {
    describe(SHOULD) {

      it("create a collection of partitions") {

        val (fieldName, values) = ("type", Seq("1", "2", "3"))
        val partitions = planInputPartitions(fieldName, values)
        partitions should have size(values.size + 1)
        val headFilter = partitions.head.getPartitionFilter
        headFilter should include (s"$fieldName lt ${values.head}")
        headFilter should include (s"$fieldName eq null")
        partitions.last.getPartitionFilter shouldBe s"$fieldName ge ${values.last}"
      }
    }
  }
}
