import BuildSupport.{constants, functions}
import sbt.librarymanagement.InclExclRule

lazy val scala212 = "2.12.18"
lazy val supportedScalaVersions = List(scala212)
lazy val compileTestDependency = "test->test;compile->compile"

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

// Dependencies versions
lazy val azureCoreOkHttpVersion = "1.11.10"
lazy val scalaTestVersion = "3.2.16"
lazy val scalaMockVersion = "5.1.0"

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

lazy val azureCoreOkHttp = ("com.azure" % "azure-core-http-okhttp" % azureCoreOkHttpVersion)
  .excludeAll(
    InclExclRule("com.fasterxml.jackson.core", "jackson-annotations"),
    InclExclRule("com.fasterxml.jackson.core", "jackson-core"),
    InclExclRule("com.fasterxml.jackson.core", "jackson-databind"),
    InclExclRule("com.fasterxml.jackson.core", "jackson-annotations"),
    InclExclRule("org.slf4j", "slf4j-api")
  )

// Test dependencies
lazy val scalactic = "org.scalactic" %% "scalactic" % scalaTestVersion
lazy val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % Test
lazy val scalaMock = "org.scalamock" %% "scalamock" % scalaMockVersion % Test

// Connector
lazy val connector = (project in file("connector"))
  .enablePlugins(sbtassembly.AssemblyPlugin)
  .settings(
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

    assembly / assemblyJarName := s"${name.value}-${version.value}.jar",
    assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(false),
    assemblyMergeStrategy := {
      case PathList("META-INF", "versions", "9", "module-info.class") => MergeStrategy.discard
      case PathList("module-info.class") => MergeStrategy.discard
      case default =>
        val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
        oldStrategy(default)
    }
  )

// Integration tests
lazy val integration = (project in file("integration"))
  .disablePlugins(sbtassembly.AssemblyPlugin)
  .dependsOn(connector % compileTestDependency)
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