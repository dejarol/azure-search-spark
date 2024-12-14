package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.LexicalAnalyzerName
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import com.github.jarol.azure.search.spark.sql.connector.core.utils.StringUtils

class AnalyzerConfigV2Spec
  extends BasicSpec {

  private lazy val objMapper = new ObjectMapper()
    .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)

  private lazy val analyzerName = LexicalAnalyzerName.BN_MICROSOFT
  private lazy val analyzerType = SearchFieldAnalyzerType.ANALYZER
  private lazy val fields = Seq("first", "second")

  describe(f"The deserialization of ${anInstanceOf[AnalyzerConfigV2]}") {
    describe(SHOULD) {
      describe("succeed when") {
        it("all properties are present and well-defined") {

          val json =
            s"""
               |{
               |  "${AnalyzerConfigV2.NAME_PROPERTY}": "${analyzerName.toString}",
               |  "${AnalyzerConfigV2.TYPE_PROPERTY}": "${analyzerType.name()}",
               |  "${AnalyzerConfigV2.FIELDS_PROPERTY}": ${fields.map(StringUtils.quoted).mkString("[",",","]")}
               |}""".stripMargin


          val instance = objMapper.readValue(json, classOf[AnalyzerConfigV2])
          instance.getName shouldBe analyzerName
          instance.getType shouldBe analyzerType
          instance.getFields should contain theSameElementsAs fields
        }

        it("analyzer type should be resolved by description") {

          val json =
            s"""
               |{
               |  "${AnalyzerConfigV2.NAME_PROPERTY}": "${analyzerName.toString}",
               |  "${AnalyzerConfigV2.TYPE_PROPERTY}": "${analyzerType.description()}",
               |  "${AnalyzerConfigV2.FIELDS_PROPERTY}": ${fields.map(StringUtils.quoted).mkString("[",",","]")}
               |}""".stripMargin


          val instance = objMapper.readValue(json, classOf[AnalyzerConfigV2])
          instance.getName shouldBe analyzerName
          instance.getType shouldBe analyzerType
          instance.getFields should contain theSameElementsAs fields
        }
      }
      describe("fail when") {
        it("the name is not given or invalid") {

          // TODO
        }

        it("the type is not given or invalid") {

          // TODO
        }

        it("fields are not given") {

          // TODO
        }
      }
    }
  }
}
