package com.github.jarol.azure.search.spark.sql.connector.write

import com.github.jarol.azure.search.spark.sql.connector.SearchITSpec
import com.github.jarol.azure.search.spark.sql.connector.core.FieldFactory
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.scalatest.{BeforeAndAfterEach, Inspectors}

class SearchWriteBuilderSpec
  extends SearchITSpec
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
      WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.KEY_FIELD_CONFIG -> idFieldName
      ) ++ options

    SearchWriteBuilder.createIndex(
      WriteConfig(allOptions),
      schema
    )

    Thread.sleep(5000)
  }

  /**
   * Assert that a field has been properly enabled/disabled when creating a new index
   * @param schema schema for index creation
   * @param fieldList name of fields to enable
   * @param config suffix for index creation property
   * @param featureAssertion feature assertion
   */

  private def assertFeatureDisabling(
                                      schema: StructType,
                                      fieldList: Seq[String],
                                      config: String,
                                      featureAssertion: FeatureAssertion
                                   ): Unit = {

    // Create index
    indexExists(testIndex) shouldBe false
    createSearchIndex(schema, Map(WriteConfig.CREATE_INDEX_PREFIX + config -> fieldList.mkString(",")))
    indexExists(testIndex) shouldBe true

    // Retrieve index fields
    val matchingFields = getIndexFields(testIndex).filter {
      p => fieldList.exists {
        _.equalsIgnoreCase(p.getName)
      }
    }

    // Assertion for matching fields
    forAll(matchingFields) {
      field =>
        if (featureAssertion.refersToDisablingFeature) {
          featureAssertion.getFeatureValue(field) shouldBe Some(false)
        } else {
          featureAssertion.getFeatureValue(field) shouldBe Some(true)
        }
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
            createArrayField("education",
              createStructType(
                createStructField("city", DataTypes.StringType),
                createStructField("title", DataTypes.StringType),
                createStructField("grade", DataTypes.IntegerType)
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

        describe("allowing the user to edit default field properties, like") {

          it("facetable") {

            val nonFacetableField = createStructField("category", DataTypes.StringType)
            val schema = createStructType(
              keyField,
              createStructField("discount", DataTypes.DoubleType),
              nonFacetableField
            )

            assertFeatureDisabling(
              schema,
              Seq(nonFacetableField.name),
              WriteConfig.DISABLE_FACETING_CONFIG,
              FeatureAssertion.FACETABLE
            )
          }

          it("filterable") {

            val nonFilterableField = createStructField("level", DataTypes.IntegerType)
            val schema = createStructType(
              keyField,
              nonFilterableField,
              createStructField("date", DataTypes.TimestampType)
            )

            assertFeatureDisabling(
              schema,
              Seq(nonFilterableField.name),
              WriteConfig.DISABLE_FILTERING_CONFIG,
              FeatureAssertion.FILTERABLE
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

            assertFeatureDisabling(
              schema,
              Seq(firstHidden.name, secondHidden.name),
              WriteConfig.HIDDEN_FIELDS_CONFIG,
              FeatureAssertion.HIDDEN
            )
          }

          it("searchable") {

            val nonSearchableField = createStructField("description", DataTypes.StringType)
            val schema = createStructType(
              keyField,
              nonSearchableField,
              createStructField("date", DataTypes.DateType)
            )

            assertFeatureDisabling(
              schema,
              Seq(nonSearchableField.name),
              WriteConfig.DISABLE_SEARCH_CONFIG,
              FeatureAssertion.SEARCHABLE
            )
          }

          it("sortable") {

            val nonSortableField = createStructField("level", DataTypes.IntegerType)
            val schema = createStructType(
              keyField,
              nonSortableField,
              createStructField(
                "address",
                createStructType(
                  createStructField("city", DataTypes.StringType)
                )
              )
            )

            assertFeatureDisabling(
              schema,
              Seq(nonSortableField.name),
              WriteConfig.DISABLE_SORTING_CONFIG,
              FeatureAssertion.SORTABLE
            )
          }
        }
      }
    }
  }
}
