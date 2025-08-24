package io.github.dejarol.azure.search.spark.connector.core.utils.json

import io.github.dejarol.azure.search.spark.connector.{BasicSpec, SearchAPIModelFactory}

class JsonBackendsSpec
  extends BasicSpec
    with SearchAPIModelFactory {

  describe(`object`[JsonBackends.type ]) {
    describe(SHOULD) {
      describe("allow the creation of several deserialization backends, like") {
        it("JSON4s") {

          // TODO
        }

        it("Azure Search REST API models") {

          // TODO
        }

        it("array of Azure Search REST API models") {

          // TODO
        }
      }
    }
  }
}
