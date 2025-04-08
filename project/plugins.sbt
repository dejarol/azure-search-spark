lazy val sonatypePluginVersion = "3.12.0"
lazy val sbtGpgPluginVersion = "2.1.2"

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % sonatypePluginVersion)
addSbtPlugin("com.github.sbt" % "sbt-pgp" % sbtGpgPluginVersion)