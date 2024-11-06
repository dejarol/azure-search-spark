package com.github.jarol.azure.search.spark.sql.connector.write

import com.github.jarol.azure.search.spark.sql.connector.SearchSpec
import com.github.jarol.azure.search.spark.sql.connector.core.FieldFactory
import com.github.jarol.azure.search.spark.sql.connector.core.schema.SearchFieldFeature
import org.apache.spark.sql.types.DataTypes
import org.scalatest.{BeforeAndAfterEach, Inspectors}

class SearchWriteBuilderSpec
  extends SearchSpec
    with FieldFactory
    with BeforeAndAfterEach
    with Inspectors {

  private lazy val idFieldName = "id"
  private lazy val testIndex = "write-builder-index"

  override def beforeEach(): Unit = {

    dropIndexIfExists(testIndex, sleep = true)
    super.beforeEach()
  }

  override def afterEach(): Unit = {

    dropIndexIfExists(testIndex, sleep = true)
    super.afterEach()
  }

  /**
   * Create a Write config
   * @param options extra options
   * @return a write config instance
   */

  private def createWriteConfig(options: Map[String, String]): WriteConfig = {

    WriteConfig(
      optionsForAuthAndIndex(testIndex) + (
        WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.KEY_FIELD -> idFieldName
      ) ++ options
    )
  }

  /**
   * Assert that a field has been properly enabled/disabled when creating a new index
   * @param names name of fields to enable
   * @param feature feature to enable
   */

  private def assertFeatureEnablingOnIndex(
                                            names: Seq[String],
                                            feature: SearchFieldFeature
                                          ): Unit = {

    // Separate expected enabled fields from expected disabled fields
    val (expectedEnabled, expectedNotEnabled) = getIndexFields(testIndex).partition {
      p => names.exists {
        _.equalsIgnoreCase(p.getName)
      }
    }

    // Assertion for expected enabled fields
    forAll(expectedEnabled) {
      field => feature.isEnabledOnField(field) shouldBe true
    }

    // Assertion for expected disabled fields
    forAll(expectedNotEnabled) {
      field => feature.isDisabledOnField(field) shouldBe true
    }
  }

  describe(`object`[SearchWriteBuilder]) {
    describe(SHOULD) {
      describe("create an index (if it does not exist)") {
        it("with as many fields as many columns") {

          val schema = createStructType(
            createStructField(idFieldName, DataTypes.StringType),
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
          SearchWriteBuilder.createIndex(createWriteConfig(Map.empty), schema)
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
            createStructField(idFieldName, DataTypes.StringType),
            createStructField("value", DataTypes.LongType),
            createStructField(actionTypeColName, DataTypes.StringType)
          )

          indexExists(testIndex) shouldBe false
          SearchWriteBuilder.createIndex(
            createWriteConfig(
              Map(
                WriteConfig.INDEX_ACTION_COLUMN_CONFIG -> actionTypeColName
              )
            ),
            schema
          )
          indexExists(testIndex) shouldBe true
          val actualFields = getIndexFields(testIndex)

          val expectedSchema = schema.filterNot {
            _.name.equalsIgnoreCase(actionTypeColName)
          }

          actualFields should have size expectedSchema.size
          actualFields.map(_.getName) should contain theSameElementsAs expectedSchema.map(_.name)
        }

        describe("allowing the user to enable field properties, like being") {

          it("facetable") {

            val facetableFields = Seq("stringValue")
            val extraOptions = Map(
              WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.FACETABLE_FIELDS -> facetableFields.mkString(",")
            )

            // TODO
          }

          it("filterable") {

            val filterableFields = Seq("stringValue")
            val extraOptions = Map(
              WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.FILTERABLE_FIELDS -> filterableFields.mkString(",")
            )

            // TODO
          }

          it("hidden") {

            val hiddenFields = Seq("intValue", "longValue")
            val extraOptions = Map(
              WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.HIDDEN_FIELDS -> hiddenFields.mkString(",")
            )

            // TODO
          }

          it("searchable") {

            val searchableFields = Seq("stringValue")
            val extraOptions = Map(
              WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.SEARCHABLE_FIELDS -> searchableFields.mkString(",")
            )

            // TODO
          }

          it("sortable") {

            val sortableFields = Seq("intValue")
            val extraOptions = Map(
              WriteConfig.CREATE_INDEX_PREFIX + WriteConfig.SORTABLE_FIELDS -> sortableFields.mkString(",")
            )

            // TODO
          }
        }
      }
    }
  }
}
