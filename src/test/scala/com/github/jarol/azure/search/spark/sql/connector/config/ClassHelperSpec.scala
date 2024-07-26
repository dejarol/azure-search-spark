package com.github.jarol.azure.search.spark.sql.connector.config

import com.github.jarol.azure.search.spark.sql.connector.BasicSpec

import java.time.LocalDate

class ClassHelperSpec
  extends BasicSpec {

  describe(anInstanceOf[ClassHelper]) {
    describe(SHOULD) {
      describe("create an instance for a given class that has") {
        it("a 0-args constructor") {

          noException shouldBe thrownBy {
            ClassHelper.createInstance(classOf[ZeroArgsBean])
          }
        }

        it("a multiple args constructor, if proper constructor arguments are provided") {

          noException shouldBe thrownBy {
            ClassHelper.createInstance(
              classOf[MultipleArgsBean],
              1.asInstanceOf[Integer],
              LocalDate.now()
            )
          }
        }
      }

      describe("throw an exception if") {
        it("fewer parameters are provided") {

          a[NoSuchMethodException] shouldBe thrownBy {
            ClassHelper.createInstance(
              classOf[MultipleArgsBean],
              LocalDate.now()
            )
          }
        }

        it("more parameters are provided") {

          a[NoSuchMethodException] shouldBe thrownBy {
            ClassHelper.createInstance(
              classOf[MultipleArgsBean],
              1.asInstanceOf[Integer],
              LocalDate.now(),
              LocalDate.now().getMonth
            )
          }
        }

        it("the same number of parameters but with different types is provided") {

          a[NoSuchMethodException] shouldBe thrownBy {
            ClassHelper.createInstance(
              classOf[MultipleArgsBean],
              1L.asInstanceOf[java.lang.Long],
              LocalDate.now()
            )
          }
        }
      }
    }
  }
}
