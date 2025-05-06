package io.github.dejarol.azure.search.spark.connector.read

import io.github.dejarol.azure.search.spark.connector.BasicSpec
import io.github.dejarol.azure.search.spark.connector.core.utils.Reflection
import io.github.dejarol.azure.search.spark.connector.read.config.ReadConfig
import io.github.dejarol.azure.search.spark.connector.read.partitioning.{EmptyJavaPartitionerFactory, SearchPartitioner}

import scala.reflect.ClassTag

class SearchBatchSpec
  extends BasicSpec {

  /**
   * Create a partitioner, by feeding [[SearchBatch.createPartitioner]] with
   *  - a [[ReadConfig]] that contains only key [[ReadConfig.PARTITIONER_CLASS_CONFIG]], with value the class name of given type
   *  - an empty array of [[org.apache.spark.sql.sources.Filter]](s)
   *
   * @tparam T type whose class name will represent the partitioner class name
   * @return either a [[SearchBatchException]] (if partitioner creation fails) or a partitioner instance
   */

  private def createPartitioner[T: ClassTag](): Either[SearchBatchException, SearchPartitioner] = {

    val classOfT = Reflection.classFromClassTag[T]
    SearchBatch.createPartitioner(
      ReadConfig(
        Map(
          ReadConfig.PARTITIONER_CLASS_CONFIG -> classOfT.getName
        )
      )
    )
  }

  describe(`object`[SearchBatch]) {
    describe(SHOULD) {
      describe("safely create a partitioner by returning") {
        it("a Right with a partitioner") {

          val result = createPartitioner[EmptyJavaPartitionerFactory]()
          result shouldBe 'right
          result.right.get.createPartitions() shouldBe empty
        }

        it(s"a Left with a ${nameOf[SearchBatchException]}") {

          createPartitioner[String]() shouldBe 'left
        }
      }
    }
  }
}
