import BuildSupport.{constants, functions}
import sbt.librarymanagement.InclExclRule
import xerial.sbt.Sonatype.sonatypeCentralHost

lazy val scala212 = "2.12.18"
lazy val supportedScalaVersions = List(scala212)
lazy val testToTestDependency = "test->test"

ThisBuild / organization := constants.ORGANIZATION
ThisBuild / description := "Azure Search Connector for Apache Spark"
ThisBuild / homepage := Some(url(constants.PROJECT_URL))
ThisBuild / licenses += (
  "Apache License 2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt")
)
ThisBuild / developers ++= constants.DEVELOPERS
ThisBuild / scmInfo := Some(
  ScmInfo(
    url(constants.PROJECT_URL),
    "scm:git:git://github.com/dejarol/azure-search-spark.git"
  )
)
ThisBuild / versionScheme := Some("early-semver")

ThisBuild / version := "0.10.1"
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

// Compile dependencies
lazy val sparkCore = "org.apache.spark" %% "spark-core" % constants.SPARK_VERSION
lazy val sparkSQL = "org.apache.spark" %% "spark-sql" % constants.SPARK_VERSION
lazy val jetbrainsAnnotation = "org.jetbrains" % "annotations" % constants.JETBRAINS_ANNOTATIONS_VERSION
lazy val azureSearchDocuments = ("com.azure" % "azure-search-documents" % constants.AZURE_SEARCH_DOCUMENTS_VERSION)
  .excludeAll(
    InclExclRule("com.fasterxml.jackson.core", "jackson-annotations"),
    InclExclRule("com.fasterxml.jackson.core", "jackson-core"),
    InclExclRule("com.fasterxml.jackson.core", "jackson-databind"),
    InclExclRule("org.slf4j", "slf4j-api")
   )

// Test dependencies
lazy val scalactic = "org.scalactic" %% "scalactic" % constants.SCALA_TEST_VERSION
lazy val scalaTest = "org.scalatest" %% "scalatest" % constants.SCALA_TEST_VERSION % Test
lazy val scalaMock = "org.scalamock" %% "scalamock" % constants.SCALA_MOCK_VERSION % Test

// Connector
lazy val connector = (project in file("connector"))
  .settings(
    name := constants.ARTIFACT_NAME,
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies ++= Seq(
      sparkCore % Provided,
      sparkSQL % Provided,
      jetbrainsAnnotation % Provided,
      azureSearchDocuments,
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

    // Publishing options
    publishMavenStyle := true,
    publishTo := sonatypePublishToBundle.value,
    sonatypeCredentialHost := sonatypeCentralHost
  )

// Integration tests
lazy val integration = (project in file("integration"))
  .dependsOn(connector % testToTestDependency)
  .settings(
    name := f"${constants.PROJECT_NAME}-integration",
    publish / skip := true,
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies ++= Seq(
      sparkCore % Test,
      sparkSQL % Test,
      jetbrainsAnnotation % Test,
      azureSearchDocuments % Test,
      scalactic,
      scalaTest,
      scalaMock
    )
  )

// Root project
lazy val root = (project in file("."))
  .aggregate(connector, integration)
  .settings(
    name := constants.PROJECT_NAME,
    crossScalaVersions := Nil
  )