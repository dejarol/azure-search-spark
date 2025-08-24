package io.github.dejarol.azure.search.spark.connector

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters
import io.github.dejarol.azure.search.spark.connector.core.schema.GeoPointType

import java.util.{List => JList}

/**
 * Mix-in trait holding some common assertions on Search fields
 */

trait FieldAssertionMixins {

  this: BasicSpec =>

  /**
   * Assert that all Search fields exist within the expected field set
   * @param actual actual set of fields
   * @param expected expected set of fields (keys are names, values are data types)
   */

  protected final def assertAllFieldsMatchNameAndDatatype(
                                                           actual: JList[SearchField],
                                                           expected: Map[String, SearchFieldDataType]
                                                         ): Unit = {

    actual should have size expected.size
    val actualMap = JavaScalaConverters.listToSeq(actual).map {
      field => (field.getName, field.getType)
    }.toMap

    actualMap should contain theSameElementsAs expected
  }

  /**
   * Assert that a Search field has been marked as complex, and that its subfields match
   * the structure of a GeoPoint
   * @param field a candidate Search field
   */

  protected final def assertIsComplexWithGeopointStructure(field: SearchField): Unit = {

    assertAllFieldsMatchNameAndDatatype(
      field.getFields,
      Map(
        GeoPointType.TYPE_LABEL -> SearchFieldDataType.STRING,
        GeoPointType.COORDINATES_LABEL -> SearchFieldDataType.collection(
          SearchFieldDataType.DOUBLE
        )
      )
    )
  }
}
