import sbt.{Attributed, File, ModuleID, URL, url}

object BuildSupport {

  object constants {

    final lazy val SPARK_VERSION = "3.1.1"
    final lazy val AZURE_SEARCH_DOCUMENTS_VERSION = "11.6.0"
    final lazy val AZURE_CORE_OKHTTP_VERSION = "1.11.10"
    final lazy val SCALA_TEST_VERSION = "3.2.16"
    final lazy val SCALA_MOCK_VERSION = "5.1.0"

    final lazy val ORGANIZATION = "io.github.dejarol"
    final lazy val ARTIFACT_NAME = "azure-search-spark-connector"
  }

  object functions {

    private lazy val SPARK_ARTIFACTS = Seq(
      "spark-core",
      "spark-sql",
      "spark-catalyst"
    )

    /**
     * Checks if the given module is a Spark module.
     * @param module The ModuleID representing the module to be checked.
     * @return True if the module is a Spark module, false otherwise.
     */

    private def isSparkModule(module: ModuleID): Boolean = {

      module.organization.equalsIgnoreCase("org.apache.spark") &&
        SPARK_ARTIFACTS.exists { prefix => module.name.startsWith(prefix) }
    }

    /**
     * Checks if the given module is the one related to the Azure Search Java client.
     * @param module The ModuleID representing the module to be checked.
     * @return True if the module is the Azure Search Java client, false otherwise.
     */

    private def isAzureSearchDocumentsModule(module: ModuleID): Boolean = {

      module.organization.equalsIgnoreCase("com.azure") &&
        module.name.startsWith("azure-search-documents")
    }

    /**
     * Creates a map of custom API mappings for a managed dependency, by inspecting the
     * project classpath (value of setting <code> Compile / fullClassPath</code>)
     * @param filesAndModules files and modules from the project classpath
     * @param modulePredicate predicate to be matched by the managed dependency
     * @param url parent url for the dependency API
     * @return a mapping with keys being dependency files and url being the link to the documentation
     */

    private def collectCustomAPIMappings(
                                          filesAndModules: Seq[(Attributed[File], ModuleID)],
                                          modulePredicate: ModuleID => Boolean,
                                          url: URL
                                        ): Map[File, URL] = {

      filesAndModules.collect {
        case (entry, module) if modulePredicate(module) =>
          (entry.data, url)
      }.toMap
    }

    /**
     * Creates a map of custom API mappings for Spark-related dependencies
     * @param filesAndModules files and modules from project classpath
     * @return custom API mappings for Spark dependencies
     */

    def collectSparkAPIMappings(filesAndModules: Seq[(Attributed[File], ModuleID)]): Map[File, URL] = {

      collectCustomAPIMappings(
        filesAndModules,
        isSparkModule,
        url(s"https://archive.apache.org/dist/spark/docs/${constants.SPARK_VERSION}/api/scala/")
      )
    }

    /**
     * Creates a map of custom API mappings for the Azure Search Java client dependency
     * @param filesAndModules files and modules from project classpath
     * @return custom API mappings for the Azure Search Java client dependency
     */

    def collectAzureSearchAPIMappings(filesAndModules: Seq[(Attributed[File], ModuleID)]): Map[File, URL] = {

      collectCustomAPIMappings(
        filesAndModules,
        isAzureSearchDocumentsModule,
        url(s"https://azuresdkdocs.blob.core.windows.net/$$web/" +
          s"java/azure-search-documents/${constants.AZURE_SEARCH_DOCUMENTS_VERSION}/")
      )
    }
  }
}