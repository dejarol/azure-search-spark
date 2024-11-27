package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.core.schema.SearchFieldFeature
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, FieldFactory, JavaScalaConverters}
import org.apache.spark.sql.types.{DataTypes, StructField}

class SearchFieldsCreationOptionsSpec
  extends BasicSpec
    with FieldFactory {

  private lazy val (first, second, third, fourth) = ("first", "second", "third", "fourth")

  private def createCompleteFeaturesMap(fields: Seq[String]): Map[SearchFieldFeature, Seq[String]] = {

    SearchFieldFeature.values().map {
      v => (v, fields)
    }.toMap
  }

  /**
   * Create an instance of [[SearchFieldsCreationOptions]]
   * @param actions feature
   * @param key name of key field
   * @param indexActionColumn index action column
   * @return an instance of [[SearchFieldsCreationOptions]]
   */

  private def createOptions(
                             actions: Map[SearchFieldFeature, Seq[String]],
                             key: String,
                             indexActionColumn: Option[String]
                           ): SearchFieldsCreationOptions = {

    SearchFieldsCreationOptions(
      key,
      disabledFromFiltering = actions.get(SearchFieldFeature.FILTERABLE),
      disabledFromSorting = actions.get(SearchFieldFeature.SORTABLE),
      hiddenFields = actions.get(SearchFieldFeature.HIDDEN),
      disabledFromSearch = actions.get(SearchFieldFeature.SEARCHABLE),
      disabledFromFaceting = actions.get(SearchFieldFeature.FACETABLE),
      indexActionColumn
    )
  }

  /**
   * Get a map with all Search index fields
   * @param options options for index creation
   * @param schema input schema
   * @return a map with keys being field names and values being field themselves
   */

  private def getSearchFieldsMap(
                                  options: SearchFieldsCreationOptions,
                                  schema: Seq[StructField]
                                ): Map[String, SearchField] = {

    options.schemaToSearchFields(schema)
      .map {
        sf => (sf.getName, sf)
      }.toMap
  }

  describe(anInstanceOf[SearchFieldsCreationOptions]) {
    describe(SHOULD) {
      it("not include index action column") {

        val schema = Seq(
          createStructField(first, DataTypes.StringType),
          createStructField(second, DataTypes.IntegerType),
          createStructField(third, DataTypes.DateType)
        )

        val indexActionColumn = fourth
        val input = schema :+ createStructField(indexActionColumn, DataTypes.StringType)
        val actual = createOptions(Map.empty, "key", Some(fourth))
          .excludeIndexActionColumn(schema)

        actual should not be empty
        val expectedFieldNames = input.collect {
          case sf if !sf.name.equalsIgnoreCase(indexActionColumn) => sf.name
        }

        val actualFieldNames = actual.map(_.name)
        actualFieldNames should contain theSameElementsAs expectedFieldNames
      }

      describe("apply some actions to Search fields, like") {
        describe("enabling or disabling a feature on") {
          it("a top-level field") {

            val schema = createStructType(
              createStructField(first, DataTypes.StringType),
              createStructField(second, DataTypes.StringType),
              createStructField(third, DataTypes.StringType)
            )

            val options = createOptions(
              createCompleteFeaturesMap(Seq(second)),
              first,
              None
            )

            val searchFields = getSearchFieldsMap(options, schema)
            searchFields(first) shouldBe enabledFor(SearchFieldFeature.KEY)
            val matchingField = searchFields(second)
            matchingField should not be enabledFor(SearchFieldFeature.FACETABLE)
            matchingField should not be enabledFor(SearchFieldFeature.FILTERABLE)
            matchingField shouldBe enabledFor(SearchFieldFeature.HIDDEN)
            matchingField should not be enabledFor(SearchFieldFeature.SEARCHABLE)
            matchingField should not be enabledFor(SearchFieldFeature.SORTABLE)
          }

          it("a nested field") {

            val schema = createStructType(
              createStructField(first, DataTypes.StringType),
              createStructField(
                second,
                createStructType(
                  createStructField(third, DataTypes.StringType),
                  createStructField("code", DataTypes.IntegerType)
                )
              )
            )

            val options = createOptions(
              createCompleteFeaturesMap(Seq(s"$second.$third")),
              first,
              None
            )

            val searchFields = getSearchFieldsMap(options, schema)
            searchFields(first) shouldBe enabledFor(SearchFieldFeature.KEY)
            val subFields = JavaScalaConverters.listToSeq(searchFields(second).getFields)
            val maybeSubField = subFields.find {
              _.getName.equalsIgnoreCase(third)
            }

            maybeSubField shouldBe defined
            val subField = maybeSubField.get
            subField should not be enabledFor(SearchFieldFeature.FACETABLE)
            subField should not be enabledFor(SearchFieldFeature.FILTERABLE)
            subField shouldBe enabledFor(SearchFieldFeature.HIDDEN)
            subField should not be enabledFor(SearchFieldFeature.SEARCHABLE)
            subField should not be enabledFor(SearchFieldFeature.SORTABLE)
          }
        }
      }
    }
  }
}
