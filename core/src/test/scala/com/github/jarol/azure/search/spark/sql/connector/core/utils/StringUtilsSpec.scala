package com.github.jarol.azure.search.spark.sql.connector.core.utils

import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, JavaScalaConverters}

class StringUtilsSpec
  extends BasicSpec {

  /**
   * Create a combined OData filter
   * @param filters filters to combine
   * @return the combined filter
   */

  private def createODataFilter(filters: Seq[String]): String = {

    StringUtils.createODataFilter(
      JavaScalaConverters.seqToList(
        filters
      )
    )
  }

  describe(`object`[StringUtils]) {
    describe(SHOULD) {
      it("surround a string both ends") {

        val input = "hello"
        StringUtils.quoted(input) shouldBe '"' + input + '"'
        StringUtils.singleQuoted(input) shouldBe s"'$input'"
      }

      it("create a combined OData filter") {

        val (first, second) = ("a eq 1", "b eq 2")
        createODataFilter(Seq.empty) shouldBe null
        createODataFilter(Seq(first)) shouldBe first
        createODataFilter(Seq(first, second)) shouldBe s"($first) and ($second)"
      }
    }
  }
}
