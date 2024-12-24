package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.LexicalAnalyzerName
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, JsonMixIns}
import com.github.jarol.azure.search.spark.sql.connector.core.utils.StringUtils

import java.util.function.Supplier
import scala.reflect.ClassTag

class AnalyzerConfigSpec
  extends BasicSpec
    with JsonMixIns {

  private lazy val analyzerName = LexicalAnalyzerName.BN_MICROSOFT
  private lazy val analyzerType = SearchFieldAnalyzerType.ANALYZER
  private lazy val fields = Seq("first", "second")

  /**
   * Assert that the generated JSON string can be deserialized as an instance of [[AnalyzerConfig]]
   *
   * @param analyzerName analyzer name
   * @param analyzerType analyzer type
   * @param fields fields
   * @param analyzerTypeToString function for converting the analyzer to string
   */

  private def assertCorrectDeserialization(
                                            analyzerName: LexicalAnalyzerName,
                                            analyzerType: SearchFieldAnalyzerType,
                                            fields: Seq[String],
                                            analyzerTypeToString: SearchFieldAnalyzerType => String
                                          ): Unit = {

    val json =
      s"""
         |{
         |  "${AnalyzerConfig.NAME_PROPERTY}": "${analyzerName.toString}",
         |  "${AnalyzerConfig.TYPE_PROPERTY}": "${analyzerTypeToString(analyzerType)}",
         |  "${AnalyzerConfig.FIELDS_PROPERTY}": ${fields.map(StringUtils.quoted).mkString("[", "," ,"]")}
         |}""".stripMargin


    val instance = readValueAs[AnalyzerConfig](json)
    instance.getName shouldBe analyzerName
    instance.getType shouldBe analyzerType
    instance.getFields should contain theSameElementsAs fields
  }

  /**
   * Assert that the deserialization of an [[AnalyzerConfig]] instance fails
   * @param analyzerName the name of the analyzer
   * @param analyzerType the type of the analyzer
   * @param fields the fields of the analyzer
   * @param messageSupplier supplier whose string should be contained by the handled exception
   * @tparam TypeEx expected exception type
   */

  private def assertDeserializationFails[TypeEx <: Throwable: ClassTag](
                                                                         analyzerName: Option[String],
                                                                         analyzerType: Option[String],
                                                                         fields: Option[Seq[String]],
                                                                         messageSupplier: Supplier[String]
                                                                       ): Unit = {

    // Create the JSON string
    val json: String = Map(
      AnalyzerConfig.NAME_PROPERTY -> analyzerName.map(StringUtils.quoted),
      AnalyzerConfig.TYPE_PROPERTY -> analyzerType.map(StringUtils.quoted),
      AnalyzerConfig.FIELDS_PROPERTY -> fields.map {
        _.map(StringUtils.quoted).mkString("[", ",", "]")
      }
    ).collect {
      case (key, Some(v)) => f"${StringUtils.quoted(key)}: $v"
    }.mkString("{", ",", "}")

    // Handle the deserialization failure
    val throwable = the [Throwable] thrownBy {
      readValueAs[AnalyzerConfig](json)
    }

    throwable shouldBe a [TypeEx]
    throwable.getLocalizedMessage should include (messageSupplier.get())
  }

  describe(f"The deserialization of ${anInstanceOf[AnalyzerConfig]}") {
    describe(SHOULD) {
      describe("succeed when") {
        it("all properties are present and well-defined") {

          assertCorrectDeserialization(
            analyzerName,
            analyzerType,
            fields,
            _.name()
          )
        }

        it("analyzer type should be resolved by description") {

          assertCorrectDeserialization(
            analyzerName,
            analyzerType,
            fields,
            _.description()
          )
        }
      }
      describe("fail when") {
        it("the name is not given or invalid") {

          assertDeserializationFails[NullPointerException](
            None,
            Some(analyzerType.name()),
            Some(fields),
            AnalyzerConfig.supplierForNonNullAnalyzerProperty(AnalyzerConfig.NAME_PROPERTY)
          )

          val invalidAnalyzerName = "hello"
          assertDeserializationFails[NoSuchElementException](
            Some(invalidAnalyzerName),
            Some(analyzerType.description()),
            Some(fields),
            () => f"Analyzer '$invalidAnalyzerName' does not exist"
          )
        }

        it("the type is not given or invalid") {

          assertDeserializationFails[NullPointerException](
            Some(analyzerName.toString),
            None,
            Some(fields),
            AnalyzerConfig.supplierForNonNullAnalyzerProperty(AnalyzerConfig.TYPE_PROPERTY)
          )

          val invalidTypeName = "hello"
          assertDeserializationFails[NoSuchElementException](
            Some(analyzerName.toString),
            Some(invalidTypeName),
            Some(fields),
            () => f"Search analyzer type '$invalidTypeName' does not exist"
          )
        }

        it("fields are not given") {

          assertDeserializationFails[NullPointerException](
            Some(analyzerName.toString),
            Some(analyzerType.name()),
            None,
            AnalyzerConfig.supplierForNonNullAnalyzerProperty(AnalyzerConfig.FIELDS_PROPERTY)
          )
        }
      }
    }
  }
}
