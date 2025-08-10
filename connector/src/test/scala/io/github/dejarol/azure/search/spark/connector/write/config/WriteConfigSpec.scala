package io.github.dejarol.azure.search.spark.connector.write.config

import com.azure.search.documents.models.IndexActionType
import io.github.dejarol.azure.search.spark.connector.BasicSpec
import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters
import io.github.dejarol.azure.search.spark.connector.core.config.ConfigException

class WriteConfigSpec
  extends BasicSpec
    with WriteConfigFactory {

  private lazy val emptyConfig: WriteConfig = WriteConfig(Map.empty[String, String])
  private lazy val (k1, k2, k3, v1, v2, v3) = ("k1", "k2", "k3", "v1", "v2", "v3")

  describe(anInstanceOf[WriteConfig]) {
    describe(SHOULD) {
      describe("retrieve") {
        it("the batch size") {

          val batchSize = 25
          emptyConfig.batchSize shouldBe WriteConfig.DEFAULT_BATCH_SIZE_VALUE
          WriteConfig(
            Map(
              WriteConfig.BATCH_SIZE_CONFIG -> s"$batchSize"
            )
          ).batchSize shouldBe batchSize
        }

        it("the index action type") {

          emptyConfig.maybeUserSpecifiedAction shouldBe empty
          emptyConfig.overallAction shouldBe WriteConfig.DEFAULT_ACTION_TYPE
          val action = IndexActionType.UPLOAD
          val configMaps: Seq[Map[String, String]] = Seq(
            action.name(),
            action.toString,
            action.name().toLowerCase,
            action.toString.toUpperCase,
            action.toString.toLowerCase,
          ).map {
            value => Map(
              WriteConfig.ACTION_CONFIG -> value
            )
          }

          forAll(configMaps) {
            configMap =>

              val wConfig = WriteConfig(configMap)
              wConfig.maybeUserSpecifiedAction shouldBe Some(action)
              wConfig.overallAction shouldBe action
          }
        }

        it("the name of the index action type column") {

          val colName = "actionCol"
          emptyConfig.actionColumn shouldBe empty
          WriteConfig(
            Map(
              WriteConfig.INDEX_ACTION_COLUMN_CONFIG -> colName
            )
          ).actionColumn shouldBe Some(colName)
        }

        it("index creation options") {

          emptyConfig.searchIndexCreationOptions.toMap shouldBe empty
          val configMap = Map(
            indexOptionKey(k1) -> v1,
            fieldOptionKey(k2) -> v2,
            indexOptionKey(k3) -> v3
          )
          val fieldCreationOptions = WriteConfig(configMap).searchIndexCreationOptions.toMap
          fieldCreationOptions should contain key k1
          fieldCreationOptions shouldNot contain key k2
          fieldCreationOptions should contain key k3
        }

        it("field enrichment options") {

          emptyConfig.searchFieldCreationOptions.toMap shouldBe empty

          val configMap = Map(
            fieldOptionKey(k1) -> v1,
            fieldOptionKey(k2) -> v2,
            k3 -> v3
          )
          val fieldCreationOptions = WriteConfig(configMap).searchFieldCreationOptions.toMap
          fieldCreationOptions should contain key k1
          fieldCreationOptions should contain key k2
          fieldCreationOptions shouldNot contain key k3
        }

        it("field to exclude from geo conversion") {

          // TODO: add test
        }
      }

      describe("upsert") {
        it("the index name") {

          // For an empty config, a ConfigException should be thrown
          a [ConfigException] should be thrownBy {
            emptyConfig.getIndex
          }

          emptyConfig.withIndexName("hello").getIndex shouldBe "hello"
        }

        it("new options (case-insensitively)") {

          val (k1, v1, v2) = ("keyOne", "v1", "v2")
          val firstOptions = Map(k1 -> v1)
          val secondOptions = Map(k1.toUpperCase -> v2)

          // No options, the result should be empty
          emptyConfig.get(k1) shouldBe empty

          // We expect to retrieve value 'v1'
          val updatedConfig = emptyConfig.withOptions(
            JavaScalaConverters.scalaMapToJava(firstOptions)
          )

          updatedConfig.get(k1) shouldBe Some(v1)

          // We expect to retrieve value 'v2'
          // (the value should have been updated event though the key is different, case-wise)
          updatedConfig.withOptions(
            JavaScalaConverters.scalaMapToJava(secondOptions)
          ).get(k1) shouldBe Some(v2)
        }
      }
    }
  }
}
