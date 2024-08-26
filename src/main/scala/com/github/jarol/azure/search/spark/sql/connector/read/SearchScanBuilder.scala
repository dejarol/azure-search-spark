package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.clients.ClientFactory
import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig
import com.github.jarol.azure.search.spark.sql.connector.schema.SchemaUtils
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.SchemaConversionRules
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.{StructField, StructType}

class SearchScanBuilder(private val schema: StructType,
                        private val readConfig: ReadConfig)
  extends ScanBuilder {

  override def build(): Scan = {

    val index: String = readConfig.getIndex
    val searchFields: Seq[SearchField] = JavaScalaConverters.listToSeq(
      ClientFactory.searchIndex(readConfig).getFields
    )

    val maybeSchemaCompatibilityException: Either[SchemaCompatibilityException, Unit] = for {
      _ <- SearchScanBuilder.allSchemaFieldsExists(schema, searchFields, index)
      _ <- SearchScanBuilder.allDataTypesAreCompatible(schema, searchFields, index)
    } yield 0

    maybeSchemaCompatibilityException match {
      case Left(value) => throw value
      case Right(_) => new SearchScan(schema, readConfig)
    }
  }
}

object SearchScanBuilder {

  protected[read] def allSchemaFieldsExists(schema: Seq[StructField],
                                            searchFields: Seq[SearchField],
                                            index: String): Either[SchemaCompatibilityException, Unit] = {

    // Detect those schema fields whose name does not match with any search field
    val schemaFieldsNotExistingOnSearchIndex: Seq[StructField] = schema.filterNot {
      structField =>
        searchFields.exists {
          searchField =>
            structField.name.equalsIgnoreCase(searchField.getName)
      }
    }

    // If there are some, throw an exception
    if (schemaFieldsNotExistingOnSearchIndex.nonEmpty) {

      val numberOfNonExistingFields = schemaFieldsNotExistingOnSearchIndex.size
      val nonExistingFieldNames = schemaFieldsNotExistingOnSearchIndex
        .map(_.name)
        .mkString("[", ",", "]")
      Left(
        new SchemaCompatibilityException(
          s"found $numberOfNonExistingFields schema field(s) that do not exist on search index $index " +
            s"$nonExistingFieldNames")
      )
    } else {
      Right()
    }
  }

  protected[read] def allDataTypesAreCompatible(schema: Seq[StructField],
                                                searchFields: Seq[SearchField],
                                                index: String): Either[SchemaCompatibilityException, Unit] = {

    // Zip search fields with their equivalent struct fields
    val searchFieldsAndStructFields = searchFields.filter {
      searchField =>
        schema.exists {
          structField => searchField.getName.equalsIgnoreCase(structField.name)
        }
    }.sortBy {
      _.getName
    }.zip(schema.sortBy {
      _.name
    })

    // Detect those tuples where there's a datatype mismatch
    val mismatchedFields = searchFieldsAndStructFields.filterNot {
      case (searchField, structField) =>
        areCompatible(searchField, structField)
    }

    if (mismatchedFields.nonEmpty) {
      val mismatchedFieldsDescription = mismatchedFields.map {
        case (sef, stf) => s"${sef.getName} (${sef.getType}, ${stf.dataType.typeName})"
      }.mkString("[", ",", "]")

      Left(
        new SchemaCompatibilityException(s"found ${mismatchedFields.size} fields with mismatched datatypes " +
          s"for index $index $mismatchedFieldsDescription")
      )
    } else {
      Right()
    }
  }

  protected[read] def areCompatible(searchField: SearchField, structField: StructField): Boolean = {

    SchemaUtils.areCompatible(
      SchemaUtils.inferSparkTypeOf(searchField),
      structField.dataType
    ) || SchemaConversionRules.existsRuleFor(
      searchField.getType,
      structField.dataType
    )
  }
}