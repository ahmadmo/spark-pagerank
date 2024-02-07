name := "spark-pagerank"
version := "1.0"
scalaVersion := "2.11.12"

resolvers ++= Seq(
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
)

scalacOptions ++= Seq(
  "-feature", "-deprecation", "-unchecked",
  "-language:implicitConversions",
  "-language:postfixOps"
)

enablePlugins(AssemblyPlugin)

val hoconVersion = "1.4.0"
val scalaLoggingVersion = "3.9.2"
val log4jVersion = "2.12.1"
val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % hoconVersion,
  "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
