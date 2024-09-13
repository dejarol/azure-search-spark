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

              val wConfig = writeConfig(configMap)
              wConfig.maybeUserSpecifiedAction shouldBe Some(action)
              wConfig.overallAction shouldBe action
          }
        }

        it("the name of the index action type column") {

          val colName = "actionCol"
          emptyConfig.actionColumn shouldBe empty
          writeConfig(
            Map(
              WriteConfig.ACTION_COLUMN_CONFIG -> colName
            )
          ).actionColumn shouldBe Some(colName)
        }

        it("the columns to convert to GeoPoints") {

          emptyConfig.convertToGeoPoints shouldBe empty

          // Empty strings
          // [a]
          writeConfig(
            Map(
              WriteConfig.CONVERT_AS_GEOPOINTS -> ""
            )
          ).convertToGeoPoints shouldBe empty

          // [b]
          writeConfig(
            Map(
              WriteConfig.CONVERT_AS_GEOPOINTS -> " "
            )
          ).convertToGeoPoints shouldBe empty

          val expected = Seq("c1", "c2")
          val actual = writeConfig(
            Map(
              WriteConfig.CONVERT_AS_GEOPOINTS -> expected.mkString(",")
            )
          ).convertToGeoPoints

          actual shouldBe defined
          actual.get should contain theSameElementsAs expected
        }
      }
    }
  }
}
