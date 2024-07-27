lazy val scala212 = "2.12.18"
lazy val scala213 = "2.13.10"
lazy val supportedScalaVersions = List(scala212, scala213)

ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := scala212
ThisBuild / compileOrder := CompileOrder.JavaThenScala
ThisBuild / javacOptions ++= "-source" :: "1.8" ::
  "-target" :: "1.8" :: Nil
ThisBuild / scalacOptions ++= "-target:jvm-1.8" ::
  "-Ywarn-unused:implicits" ::
  "-Ywarn-unused:imports" ::
  "-Ywarn-unused:locals" ::
  "-Ywarn-unused:params" ::
  "-Ywarn-unused:privates" :: Nil

ThisBuild / test / parallelExecution := false
ThisBuild / test / logBuffered := false
ThisBuild / Test / testOptions += Tests.Argument("-oD")
ThisBuild / assembly / assemblyJarName := s"${name.value}-${version.value}.jar"
ThisBuild / assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(false)

// Dependencies versions
lazy val sparkVersion = "3.3.0"
lazy val azureSearchVersion = "11.6.0"
lazy val azureCoreOkHttpVersion = "1.11.10"

lazy val scalaTestVersion = "3.2.16"

// Compile dependencies
lazy val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion % Provided
lazy val sparkSQL = "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
lazy val azureSearchClient = ("com.azure" % "azure-search-documents" % azureSearchVersion)
  .exclude("com.azure", "azure-core-http-netty")

lazy val azureCoreOkHttp = "com.azure" % "azure-core-http-okhttp" % azureCoreOkHttpVersion

// Test dependencies
lazy val scalactic = "org.scalactic" %% "scalactic" % scalaTestVersion
lazy val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % Test

lazy val root = (project in file("."))
  .settings(
    name := "azure-search-spark",
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies ++= Seq(
      sparkCore,
      sparkSQL,
      azureSearchClient,
      azureCoreOkHttp,
      scalactic,
      scalaTest
    )
  )
