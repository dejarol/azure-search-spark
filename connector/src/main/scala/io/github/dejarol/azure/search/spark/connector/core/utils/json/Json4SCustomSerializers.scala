package io.github.dejarol.azure.search.spark.connector.core.utils.json

import com.azure.search.documents.indexes.models.LexicalAnalyzerName
import org.json4s.JsonAST.JString
import org.json4s.{CustomSerializer, JValue}

/**
 * Object holding factory methods for creating custom JSON4s serializers (e.g. for custom types)
 * that one can later add to default JSON4s formats
 * @since 0.12.0
 */

object Json4SCustomSerializers {

  /**
   * Create a [[org.json4s.CustomSerializer]] for a given type, providing partial functions for both serialization and deserialization
   * @param deserializationPF partial function for deserialization
   * @param serializationPF partial function for serialization
   * @tparam A target type (should have an implicit <code>Manifest</code> in scope)
   * @return a custom serializer for the given type
   */

  private def serializerFor[A: Manifest](
                                          deserializationPF: PartialFunction[JValue, A],
                                          serializationPF: PartialFunction[Any, JValue]
                                        ): CustomSerializer[A] = {

    new CustomSerializer[A](
      _ => (
        deserializationPF,
        serializationPF
      )
    )
  }

  /**
   * Create a custom serializer for [[com.azure.search.documents.indexes.models.LexicalAnalyzerName]]
   * @return a custom serializer for lexical analyzers
   */

  def forLexicalAnalyzerName: CustomSerializer[LexicalAnalyzerName] = {

    // Serialize a string to a LexicalAnalyzerName instance if the name matches
    val serializationPF: PartialFunction[JValue, LexicalAnalyzerName] = {
      case JString(s) if LexicalAnalyzerName.values().stream().anyMatch(
        (t: LexicalAnalyzerName) => t.toString.equalsIgnoreCase(s)
      ) =>
        LexicalAnalyzerName.fromString(s)
    }

    // Deserialize a LexicalAnalyzerName instance to a string
    val deserializationPF: PartialFunction[Any, JValue] = {
      case x: LexicalAnalyzerName => JString(x.toString)
    }

    serializerFor[LexicalAnalyzerName](
      serializationPF, deserializationPF
    )
  }
}
