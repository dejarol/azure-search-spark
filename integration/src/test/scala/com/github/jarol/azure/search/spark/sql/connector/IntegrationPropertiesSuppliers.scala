package com.github.jarol.azure.search.spark.sql.connector

import java.util.Properties

object IntegrationPropertiesSuppliers {

  private object EnvSupplier
    extends IntegrationPropertiesSupplier {

    override def endPoint(): String = getPropertyOfThrow("AZURE_SEARCH_ENDPOINT")
    override def apiKey(): String = getPropertyOfThrow("AZURE_SEARCH_API_KEY")

    private def getPropertyOfThrow(key: String): String = {

      sys.env.get(key) match {
        case Some(value) => value
        case None => throw new IllegalStateException(
          s"A ${this.getClass.getSimpleName} has been configured, " +
            s"but env property $key does not exist. Please define such and property and then repeat the test")
      }
    }
  }

  private case class FileSupplier(private val properties: Properties)
    extends IntegrationPropertiesSupplier {

    override def endPoint(): String = properties.getProperty("azure.search.endpoint")
    override def apiKey(): String = properties.getProperty("azure.search.apiKey")
  }
}
