import sbt.{Attributed, File, ModuleID, URL, url}

object BuildSupport {

  object constants {

    final val SPARK_VERSION = "3.1.1"
    final val AZURE_SEARCH_DOCUMENTS_VERSION = "11.6.0"
    final val ARTIFACT_NAME = "azure-search-spark-connector"
  }

  object functions {

    private lazy val SPARK_ARTIFACTS = Seq(
      "spark-core",
      "spark-sql",
      "spark-catalyst"
    )

    private def isSparkModule(module: ModuleID): Boolean = {

      module.organization.equalsIgnoreCase("org.apache.spark") &&
        SPARK_ARTIFACTS.exists {
          prefix => module.name.startsWith(prefix)
        }
    }

    private def isAzureSearchDocumentsModule(module: ModuleID): Boolean = {

      module.organization.equalsIgnoreCase("com.azure") &&
        module.name.startsWith("azure-search-documents")
    }

    def collectSparkAPIMappings(filesAndModules: Seq[(Attributed[File], ModuleID)]): Map[File, URL] = {

      filesAndModules.collect {
        case (entry, module) if isSparkModule(module) => (
          entry.data,
          url(s"https://archive.apache.org/dist/spark/docs/${constants.SPARK_VERSION}/api/scala/")
        )
      }.toMap
    }

    def collectAzureSearchAPIMappings(filesAndModules: Seq[(Attributed[File], ModuleID)]): Map[File, URL] = {

      filesAndModules.collect {
        case (entry, module) if isAzureSearchDocumentsModule(module) => (
          entry.data,
          url("https://azuresdkdocs.blob.core.windows.net/$web/java/azure-search-documents/11.6.0/")
        )
      }.toMap
    }
  }
}
