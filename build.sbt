import BuildSupport.{constants, functions}
import sbt.librarymanagement.InclExclRule

lazy val scala212 = "2.12.18"
lazy val supportedScalaVersions = List(scala212)
lazy val testToTestDependency = "test->test"

ThisBuild / version := "0.7.0"
ThisBuild / scalaVersion := scala212
ThisBuild / compileOrder := CompileOrder.JavaThenScala
ThisBuild / javacOptions ++= Seq(
  "-source", "1.8",
  "-target", "1.8"
)

ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-target:jvm-1.8",
  "-Ywarn-unused:implicits",
  "-Ywarn-unused:imports",
  "-Ywarn-unused:locals",
  "-Ywarn-unused:params",
  "-Ywarn-unused:privates"
)

ThisBuild / autoAPIMappings := true
ThisBuild / test / parallelExecution := false
ThisBuild / test / logBuffered := false
ThisBuild / Test / testOptions += Tests.Argument("-oD")

ThisBuild / organization := constants.ORGANIZATION

// Compile dependencies
lazy val sparkCore = "org.apache.spark" %% "spark-core" % constants.SPARK_VERSION % Provided
lazy val sparkSQL = "org.apache.spark" %% "spark-sql" % constants.SPARK_VERSION % Provided
lazy val azureSearchDocuments = ("com.azure" % "azure-search-documents" % constants.AZURE_SEARCH_DOCUMENTS_VERSION)
  .excludeAll(
    InclExclRule("com.azure", "azure-core-http-netty"),
    InclExclRule("com.fasterxml.jackson.core", "jackson-annotations"),
    InclExclRule("com.fasterxml.jackson.core", "jackson-core"),
    InclExclRule("com.fasterxml.jackson.core", "jackson-databind"),
    InclExclRule("com.fasterxml.jackson.core", "jackson-annotations"),
    InclExclRule("org.slf4j", "slf4j-api")
   )

lazy val azureCoreOkHttp = ("com.azure" % "azure-core-http-okhttp" % constants.AZURE_CORE_OKHTTP_VERSION)
  .excludeAll(
    InclExclRule("com.fasterxml.jackson.core", "jackson-annotations"),
    InclExclRule("com.fasterxml.jackson.core", "jackson-core"),
    InclExclRule("com.fasterxml.jackson.core", "jackson-databind"),
    InclExclRule("com.fasterxml.jackson.core", "jackson-annotations"),
    InclExclRule("org.slf4j", "slf4j-api")
  )

// Test dependencies
lazy val scalactic = "org.scalactic" %% "scalactic" % constants.SCALA_TEST_VERSION
lazy val scalaTest = "org.scalatest" %% "scalatest" % constants.SCALA_TEST_VERSION % Test
lazy val scalaMock = "org.scalamock" %% "scalamock" % constants.SCALA_MOCK_VERSION % Test

// Connector
lazy val connector = (project in file("connector"))
  .enablePlugins(sbtassembly.AssemblyPlugin)
  .settings(
    name := constants.ARTIFACT_NAME,
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies ++= Seq(
      sparkCore,
      sparkSQL,
      azureSearchDocuments,
      azureCoreOkHttp,
      scalactic,
      scalaTest,
      scalaMock
    ),

    // Enrich API mappings for scaladoc generation
    apiMappings ++= {

      val filesAndModules: Seq[(Attributed[File], ModuleID)] = (Compile / fullClasspath)
        .value.flatMap {
          entry => entry.get(moduleID.key).map(entry -> _)
        }

      functions.collectSparkAPIMappings(filesAndModules) ++
        functions.collectAzureSearchAPIMappings(filesAndModules)
    },

    // Assembly settings
    assembly / assemblyJarName := s"${constants.ARTIFACT_NAME}_${scalaBinaryVersion.value}-${version.value}.jar",
    assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(false),
    assemblyMergeStrategy := {
      case PathList("META-INF", "versions", "9", "module-info.class") => MergeStrategy.first
      case PathList("module-info.class") => MergeStrategy.first
      case default =>
        val oldStrategy = assemblyMergeStrategy.value
        oldStrategy(default)
    },

    // Publishing options
    publishMavenStyle := true
  )

// Integration tests
lazy val integration = (project in file("integration"))
  .disablePlugins(sbtassembly.AssemblyPlugin)
  .dependsOn(connector % testToTestDependency)
  .settings(
    publish / skip := true,
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies ++= Seq(
      sparkCore,
      sparkSQL,
      azureSearchDocuments,
      azureCoreOkHttp
    )
  )

// Root project
lazy val root = (project in file("."))
  .aggregate(connector, integration)
  .settings(
    crossScalaVersions := Nil
  )