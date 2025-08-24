package io.github.dejarol.azure.search.spark.connector

import java.util.Properties
import java.lang.{Boolean => JBoolean}
import scala.io.Source
import scala.util.Try

/**
 * Collection of instances of [[IntegrationPropertiesSupplier]]
 */

object IntegrationPropertiesSuppliers {

  final val CI_CD_ENV_VAR = "IS_CI_CD_ENV"

  /**
   * Supplier that retrieves integration properties by reading env variables
   */

  private object EnvSupplier
    extends IntegrationPropertiesSupplier {

    override def endPoint(): String = getPropertyOfThrow("AZURE_SEARCH_ENDPOINT")
    override def apiKey(): String = getPropertyOfThrow("AZURE_SEARCH_API_KEY")

    private def getPropertyOfThrow(key: String): String = {

      sys.env.get(key) match {
        case Some(value) => value
        case None => throw new IllegalStateException(
          s"A ${this.getClass.getSimpleName} has been configured, " +
            s"but env property $key does not exist. Please define such property and then repeat the test")
      }
    }
  }

  /**
   * Supplier that retrieves integration properties by reading a local .secret file
   * @param properties properties from the secret file
   */

  private case class FileSupplier(private val properties: Properties)
    extends IntegrationPropertiesSupplier {

    override def endPoint(): String = properties.getProperty("azure.search.endpoint")
    override def apiKey(): String = properties.getProperty("azure.search.apiKey")
  }

  /**
   * Resolve which instance of [[IntegrationPropertiesSupplier]] should be used, by reading the value of env variable
   * <code>IS_CI_CD_ENV</code>. If such variable is set to <code>true</code>, it will return a supplier that retrieves
   * integration properties by reading env variables. Otherwise, it will return a supplier that retrieves integration
   * properties by reading a local .secrets file
   * @return an instance of [[IntegrationPropertiesSupplier]]
   */

  final def resolve(): IntegrationPropertiesSupplier = {

    // Detect if we're on CI/CD by reading an env variable
    val isCICDEnv = sys.env.get(CI_CD_ENV_VAR).exists(JBoolean.parseBoolean)
    if (isCICDEnv) {
      EnvSupplier
    } else {
      createSecretSupplier(".integration.secrets")
    }
  }

  /**
   * Create an instance of [[IntegrationPropertiesSupplier]] by reading a local .secret file
   * <br>
   * Used for executing single integration tests in a local environment
   * @param fileName secret file name
   * @return an instance of [[IntegrationPropertiesSupplier]] that will retrieve properties from a local secret file
   */

  //noinspection SameParameterValue
  private def createSecretSupplier(fileName: String): IntegrationPropertiesSupplier = {

    Try {
      val properties = new Properties()
      properties.load(Source.fromFile(fileName).reader())
      properties
    }.toEither.right.map(FileSupplier) match {
      case Left(value) => throw new IllegalStateException(
        s"Could not load local secrets file $fileName. " +
          s"You should either create a file named $fileName at project root " +
          s"or set env variable $CI_CD_ENV_VAR to 'true' to retrieve properties from env variables", value)
      case Right(value) => value
    }
  }
}
