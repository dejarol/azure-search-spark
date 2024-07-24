package com.github.jarol.azure.search.spark.sql.connector.schema

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.JavaToScala
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, StructField, StructType}

import scala.util.matching.Regex

object SchemaUtils {

  final val COLLECTION_PATTERN: Regex = "^Collection\\(([\\w.]+)\\)$".r

  final val SIMPLE_TYPES: Map[SearchFieldDataType, DataType] = Map(
    SearchFieldDataType.STRING ->  DataTypes.StringType,
    SearchFieldDataType.INT32 -> DataTypes.IntegerType,
    SearchFieldDataType.INT64 -> DataTypes.LongType,
    SearchFieldDataType.DOUBLE -> DataTypes.DoubleType,
    SearchFieldDataType.BOOLEAN -> DataTypes.BooleanType,
    SearchFieldDataType.DATE_TIME_OFFSET -> DataTypes.TimestampType,
    SearchFieldDataType.SINGLE -> DataTypes.FloatType
  )

  protected [schema] def isSimpleType(searchType: SearchFieldDataType): Boolean = SIMPLE_TYPES.contains(searchType)
  protected [schema] def isComplexType(searchType: SearchFieldDataType): Boolean = SearchFieldDataType.COMPLEX.equals(searchType)
  protected [schema] def isCollectionType(searchType: SearchFieldDataType): Boolean = COLLECTION_PATTERN.findFirstMatchIn(searchType.toString).isDefined

  protected [schema] def extractCollectionSubType(searchType: SearchFieldDataType): SearchFieldDataType = {
    COLLECTION_PATTERN.findFirstMatchIn(searchType.toString).map {
      `match` => SearchFieldDataType.fromString(`match`.group(1))
    } match {
      case Some(value) => value
      case None => throw new IllegalStateException("")
    }
  }

  def asSchema(fields: Iterable[SearchField]): StructType = {

    StructType(
      fields.map {
        searchFieldAsStructField
      }.toSeq
    )
  }

  def searchFieldAsStructField(searchField: SearchField): StructField = {

    StructField(
      searchField.getName,
      resolveSearchDataType(searchField),
      nullable = true
    )
  }

  def resolveSearchDataType(searchField: SearchField): DataType = {

    val searchType = searchField.getType
    if (isSimpleType(searchType)) {
      SIMPLE_TYPES(searchType)
    } else if (isComplexType(searchType)) {

      // Extract subfields, convert them into StructFields and collect them into a StructType
      StructType(JavaToScala.listToSeq(searchField.getFields)
        .map(searchFieldAsStructField)
      )
    } else if (isCollectionType(searchType)) {
      ArrayType(
        resolveSearchDataType(
          new SearchField(null, extractCollectionSubType(searchType))),
        containsNull = true
      )
    } else {
      throw new IllegalStateException(f"Unsupported datatype $searchType")
    }
  }
}
