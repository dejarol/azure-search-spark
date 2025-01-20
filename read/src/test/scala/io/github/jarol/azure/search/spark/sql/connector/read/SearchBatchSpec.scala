package io.github.jarol.azure.search.spark.sql.connector.read

import io.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import io.github.jarol.azure.search.spark.sql.connector.core.utils.Reflection
import io.github.jarol.azure.search.spark.sql.connector.read.config.ReadConfig
import io.github.jarol.azure.search.spark.sql.connector.read.partitioning.{EmptyPartitioner, SearchPartitioner}

import scala.reflect.ClassTag

class SearchBatchSpec
  extends BasicSpec {

  /**
   * Create a partitioner, by feeding [[SearchBatch$.createPartitioner]] with
   *  - a [[ReadConfig]] that contains only key [[ReadConfig.PARTITIONER_CLASS_CONFIG]], with value the class name of given type
   *  - an empty array of [[org.apache.spark.sql.connector.expressions.filter.Predicate]](s)
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

  /**
   * Assert that the partitioner creation failed, when using the class name of a type as value for [[ReadConfig.PARTITIONER_CLASS_CONFIG]]
   * @tparam T type whose class name will represent the partitioner class name
   */

  private def assertPartitionerCreationFailed[T: ClassTag](): Unit = createPartitioner[T]() shouldBe 'left

  /**
   * Assert that the partitioner creation succeeded, and then perform a further assertion on the created partitioner
   * @param assertion assertion on created partitioner
   * @tparam T partitioner type (should extend [[SearchPartitioner]])
   */

  private def assertPartitionerCreationSucceeded[T <: SearchPartitioner: ClassTag](assertion: SearchPartitioner => Unit): Unit = {

    val result = createPartitioner[T]()
    result shouldBe 'right
    assertion(result.right.get)
  }

  describe(`object`[SearchBatch]) {
    describe(SHOULD) {
      it("safely create a partitioner") {

        assertPartitionerCreationFailed[String]()
        assertPartitionerCreationSucceeded[EmptyPartitioner] {
          _ shouldBe an[EmptyPartitioner]
        }
      }
    }
  }
}
