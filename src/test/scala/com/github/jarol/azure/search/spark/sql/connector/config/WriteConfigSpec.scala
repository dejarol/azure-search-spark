package com.github.jarol.azure.search.spark.sql.connector.config

class WriteConfigSpec
  extends ConfigSpec {

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
      }
    }
  }
}
