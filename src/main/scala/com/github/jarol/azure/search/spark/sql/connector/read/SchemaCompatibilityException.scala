package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.AzureSparkException
import org.apache.spark.sql.types.StructField

class SchemaCompatibilityException(missingFields: Seq[String],
                                   nonCompatibleFields: Map[StructField, SearchField])
  extends AzureSparkException {

  override def getMessage: String = {

    val missingFieldDescription: Option[String] = if (missingFields.isEmpty) None else {
      Some(
        s"${missingFields.size} missing fields " +
          s"${missingFields.mkString("(", ",", ")")})"
      )
    }

    val nonCompatibleFieldsDescription: Option[String] = if (nonCompatibleFields.isEmpty) None else {

      val tuplesDescription = nonCompatibleFields.map {
        case (k, v) => s"${k.name} (spark type: ${k.dataType.typeName}, search type: ${v.getType})"
      }.mkString("(", ",", ")")

      Some(
        s"${nonCompatibleFields.size} fields with non-compatible data types $tuplesDescription"
      )
    }

    (missingFieldDescription, nonCompatibleFieldsDescription) match {
      case (Some(a), Some(b)) => s"Found $a and $b"
      case (Some(a), None) => s"Found $a"
      case (None, Some(b)) => s"Found $b"
      case _ => "No missing or non-compatible fields"
    }
  }
}
