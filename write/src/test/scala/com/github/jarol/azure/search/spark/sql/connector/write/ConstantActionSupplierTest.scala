package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.models.IndexActionType
import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec

class ConstantActionSupplierTest
  extends BasicSpec {

  describe(anInstanceOf[ConstantActionSupplier]) {
    describe(SHOULD) {
      it("supply a constant value") {

        val default = IndexActionType.MERGE
        new ConstantActionSupplier(
          default
        ).get(
          InternalRow(Seq.empty: _*)
        ) shouldBe default
      }
    }
  }
}
