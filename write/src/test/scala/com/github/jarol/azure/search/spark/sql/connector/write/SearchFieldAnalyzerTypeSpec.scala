package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.{LexicalAnalyzerName, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, FieldFactory}
import org.scalatest.Inspectors

class SearchFieldAnalyzerTypeSpec
  extends BasicSpec
    with FieldFactory
      with Inspectors {

  private lazy val field = createSearchField("hello", SearchFieldDataType.STRING)
  private lazy val analyzerName = LexicalAnalyzerName.BG_LUCENE

  describe(anInstanceOf[SearchFieldAnalyzerType]) {
    describe(SHOULD) {
      it("set and get an analyzer") {

        forAll(SearchFieldAnalyzerType.values().toSeq) {
          value =>
            value.getFromField(field) shouldBe null
            val fieldWithAnalyzer = value.setterAction(analyzerName).apply(field)
            value.getFromField(fieldWithAnalyzer) shouldBe analyzerName
        }
      }
    }
  }
}
