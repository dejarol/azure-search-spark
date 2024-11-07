package com.github.jarol.azure.search.spark.sql.connector.write

import com.github.jarol.azure.search.spark.sql.connector.SearchSpec
import com.github.jarol.azure.search.spark.sql.connector.core.FieldFactory
import com.github.jarol.azure.search.spark.sql.connector.core.schema.SearchFieldFeature
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.scalatest.{BeforeAndAfterEach, Inspectors}

class SearchWriteBuilderSpec
  extends SearchSpec
    with FieldFactory
    with BeforeAndAfterEach
    with Inspectors {

  private lazy val idFieldName = "id"
  private lazy val testIndex = "write-builder-index"
  private lazy val keyField = createStructField(idFieldName, DataTypes.StringType)

  /**
   * Delete index used for integration testing
   */

  override def afterEach(): Unit = {

    dropIndexIfExists(testIndex, sleep = true)
    super.afterEach()
  }

  private def createSearchIndex(
                                 schema: StructType,
                                 options: Map[String, String]
                               ): Unit = {

    // Take options for auth and index,
    // add key field and provided options
    val allOptions: Map[String, String] = optionsForAuthAndIndex(testIndex) + (
      WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.KEY_FIELD -> idFieldName
      ) ++ options

    SearchWriteBuilder.createIndex(
      WriteConfig(allOptions),
      schema
    )
  }

  /**
   * Assert that a field has been properly enabled/disabled when creating a new index
   * @param fieldsToEnable name of fields to enable
   * @param feature feature to enable
   */

  private def assertFeatureEnabling(
                                     schema: StructType,
                                     fieldsToEnable: Seq[String],
                                     config: String,
                                     feature: SearchFieldFeature
                                   ): Unit = {

    // Create index
    indexExists(testIndex) shouldBe false
    createSearchIndex(schema, Map(WriteConfig.CREATE_INDEX_PREFIX + config -> fieldsToEnable.mkString(",")))
    indexExists(testIndex) shouldBe true

    // Retrieve index fields,
    // separate expected enabled fields from expected disabled fields
    val (expectedEnabled, expectedNotEnabled) = getIndexFields(testIndex).partition {
      p => fieldsToEnable.exists {
        _.equalsIgnoreCase(p.getName)
      }
    }

    // Assertion for expected enabled fields
    forAll(expectedEnabled) {
      field =>
        feature.isEnabledOnField(field) shouldBe true
    }

    // Assertion for expected disabled fields
    forAll(expectedNotEnabled) {
      field =>
        feature.isDisabledOnField(field) shouldBe true
    }
  }

  describe(`object`[SearchWriteBuilder]) {
    describe(SHOULD) {
      describe("create an index") {
        it("with as many fields as many columns") {

          val schema = createStructType(
            keyField,
            createStructField("name", DataTypes.StringType),
            createStructField("date", DataTypes.TimestampType),
            createStructField("address",
              createStructType(
                createStructField("city", DataTypes.StringType),
                createStructField("code", DataTypes.IntegerType)
              )
            )
          )

          indexExists(testIndex) shouldBe false
          createSearchIndex(schema, Map.empty)
          indexExists(testIndex) shouldBe true
          val actualFields = getIndexFields(testIndex)

          actualFields should have size schema.size
          val expectedFieldNames = schema.map(_.name)
          val actualFieldNames = actualFields.map(_.getName)
          actualFieldNames should contain theSameElementsAs expectedFieldNames
        }

        it("not including the column used for index action type") {

          val actionTypeColName = "actionType"
          val schema = createStructType(
            keyField,
            createStructField("value", DataTypes.LongType),
            createStructField(actionTypeColName, DataTypes.StringType)
          )

          indexExists(testIndex) shouldBe false
          createSearchIndex(
            schema,
            Map(
              WriteConfig.INDEX_ACTION_COLUMN_CONFIG -> actionTypeColName
            )
          )
          indexExists(testIndex) shouldBe true
          val actualFields = getIndexFields(testIndex)

          val expectedSchema = schema.filterNot {
            _.name.equalsIgnoreCase(actionTypeColName)
          }

          actualFields should have size expectedSchema.size
          actualFields.map(_.getName) should contain theSameElementsAs expectedSchema.map(_.name)
        }

        describe("allowing the user to enable field properties, like") {

          it("facetable") {

            val facetableField = createStructField("category", DataTypes.StringType)
            val schema = createStructType(
              keyField,
              createStructField("discount", DataTypes.DoubleType),
              facetableField
            )

            assertFeatureEnabling(
              schema,
              Seq(facetableField.name),
              WriteConfig.FACETABLE_FIELDS,
              SearchFieldFeature.FACETABLE
            )
          }

          it("filterable") {

            val filterableField = createStructField("level", DataTypes.IntegerType)
            val schema = createStructType(
              keyField,
              filterableField,
              createStructField("date", DataTypes.TimestampType)
            )

            assertFeatureEnabling(
              schema,
              Seq(filterableField.name),
              WriteConfig.FILTERABLE_FIELDS,
              SearchFieldFeature.FILTERABLE
            )
          }

          it("hidden") {

            val firstHidden = createStructField("first", DataTypes.IntegerType)
            val secondHidden = createStructField("second", DataTypes.TimestampType)
            val schema = createStructType(
              keyField,
              firstHidden,
              secondHidden,
              createStructField("category", DataTypes.StringType)
            )

            assertFeatureEnabling(
              schema,
              Seq(firstHidden.name, secondHidden.name),
              WriteConfig.HIDDEN_FIELDS,
              SearchFieldFeature.HIDDEN
            )
          }

          it("searchable") {

            val searchableField = createStructField("description", DataTypes.StringType)
            val schema = createStructType(
              keyField,
              searchableField,
              createStructField("date", DataTypes.DateType)
            )

            assertFeatureEnabling(
              schema,
              Seq(searchableField.name),
              WriteConfig.SEARCHABLE_FIELDS,
              SearchFieldFeature.SEARCHABLE
            )
          }

          it("sortable") {

            val sortableField = createStructField("level", DataTypes.IntegerType)
            val schema = createStructType(
              keyField,
              sortableField,
              createStructField(
                "address",
                createStructType(
                  createStructField("city", DataTypes.StringType)
                )
              )
            )

            assertFeatureEnabling(
              schema,
              Seq(sortableField.name),
              WriteConfig.SORTABLE_FIELDS,
              SearchFieldFeature.SORTABLE
            )
          }
        }
      }
    }
  }
}
