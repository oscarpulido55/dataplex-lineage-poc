name := "lineage-poc"

version := "1.0"

scalaVersion := "2.12.18"

val sparkVersion = "3.5.0"

dependencyOverrides += "com.google.guava" % "guava" % "32.1.3-jre"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",

  // Google Cloud Spark Connector for BigQuery
  "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.35.0" % "provided",

  // Google Auth Library for REST API calls
  "com.google.auth" % "google-auth-library-oauth2-http" % "1.20.0" % "provided"
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
