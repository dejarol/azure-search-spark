package com.github.jarol.azure.search.spark.sql.connector.core.utils

import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec

class StringUtilsSpec
  extends BasicSpec {

  describe(`object`[StringUtils]) {
    describe(SHOULD) {
      it("surround a string both ends") {

        val input = "hello"
        StringUtils.quoted(input) shouldBe '"' + input + '"'
        StringUtils.singleQuoted(input) shouldBe s"'$input'"
      }
    }
  }
}
