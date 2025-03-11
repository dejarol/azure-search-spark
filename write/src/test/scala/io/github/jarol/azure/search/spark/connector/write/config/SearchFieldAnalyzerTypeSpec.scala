package io.github.jarol.azure.search.spark.connector.write.config

import com.azure.search.documents.indexes.models.{LexicalAnalyzerName, SearchFieldDataType}
import io.github.jarol.azure.search.spark.connector.core.{BasicSpec, FieldFactory}

class SearchFieldAnalyzerTypeSpec
  extends BasicSpec
    with FieldFactory {

  private lazy val field = createSearchField("hello", SearchFieldDataType.STRING)
  private lazy val analyzerName = LexicalAnalyzerName.BG_LUCENE

  describe(anInstanceOf[SearchFieldAnalyzerType]) {
    describe(SHOULD) {
      it("set and get an analyzer") {

        forAll(SearchFieldAnalyzerType.values().toSeq) {
          value =>
            value.getFromField(field) shouldBe null
            val fieldWithAnalyzer = value.setOnField(field, analyzerName)
            value.getFromField(fieldWithAnalyzer) shouldBe analyzerName
        }
      }
    }
  }
}
