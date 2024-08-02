package com.github.jarol.azure.search.spark.sql.connector.config

import com.azure.search.documents.models.IndexActionType
import org.scalatest.Inspectors

class WriteConfigSpec
  extends ConfigSpec
    with Inspectors {

  private lazy val emptyConfig: WriteConfig = writeConfig(Map.empty)

  describe(anInstanceOf[WriteConfig]) {
    describe(SHOULD) {
      describe("retrieve") {
        it("the batch size") {

          val batchSize = 25
          emptyConfig.batchSize shouldBe WriteConfig.DEFAULT_BATCH_SIZE_VALUE
          writeConfig(
            Map(
              WriteConfig.BATCH_SIZE_CONFIG -> s"$batchSize"
            )
          ).batchSize shouldBe batchSize
        }

        it("the index action type") {

          emptyConfig.indexAction shouldBe empty
          val indexActionType = IndexActionType.MERGE_OR_UPLOAD
          val configs: Seq[Map[String, String]] = Seq(
            indexActionType.name(),
            indexActionType.toString,
            indexActionType.name().toLowerCase,
            indexActionType.toString.toUpperCase,
            indexActionType.toString.toLowerCase,
          ).map {
            value => Map(
              WriteConfig.ACTION_CONFIG -> value
            )
          }

          forAll(configs) {
            config => writeConfig(
              config
            ).indexAction shouldBe Some(indexActionType)
          }
        }
      }
    }
  }
}
