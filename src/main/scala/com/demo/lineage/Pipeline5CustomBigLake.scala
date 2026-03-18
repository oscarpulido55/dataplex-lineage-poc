package com.demo.lineage

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.util.UUID
import java.net.HttpURLConnection
import java.net.URL
import java.io.{BufferedReader, InputStreamReader, OutputStreamWriter}
import java.time.Instant
import java.util.regex.Pattern

object Pipeline5CustomBigLake {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: Pipeline5CustomBigLake <projectId> <bucketBlmsData> <catalogName> <region>")
      System.exit(1)
    }

    val projectId = args(0)
    val bucketBlmsData = args(1)
    val catalogName = args(2)
    val region = args(3)

    println(s"Pipeline 5 V2: Initializing with Project=$projectId, DataBucket=$bucketBlmsData, Catalog=$catalogName")

    val spark = SparkSession.builder()
      .appName("Pipeline 5 V2: Custom BigLake Rest Catalog")
      .getOrCreate()

    // 1. Source Data
    val sourceGcs = s"gs://$bucketBlmsData/source/"
    println(s"Reading data from: $sourceGcs")
    val dfRaw = spark.read.parquet(sourceGcs)

    // 2. Setup Namespace
    val namespace = "pipeline5_namespace_v2"
    val namespaceLocation = s"gs://$bucketBlmsData/$namespace"
    println(s"Creating Namespace `$catalogName`.`$namespace` at $namespaceLocation in region $region")
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS `$catalogName`.`$namespace` LOCATION '$namespaceLocation' WITH DBPROPERTIES ('gcp-region' = '$region')")

    // 3. Write Step 1
    val step1Df = dfRaw.withColumn("ingestion_timestamp", current_timestamp())
                       .withColumn("pipeline_id", lit("pipeline_5_v2_step_1"))
    val table1 = "pipeline5_step1_v2"
    val table1Name = s"`$catalogName`.`$namespace`.$table1"
    println(s"Step 1: Writing to $table1Name")
    spark.sql(s"CREATE TABLE IF NOT EXISTS $table1Name (${step1Df.schema.toDDL}) USING iceberg")
    step1Df.writeTo(table1Name).append()

    // 4. Read Step 1 and Write Step 2
    println(s"Reading from: $table1Name")
    val dfIceberg1 = spark.table(table1Name)
    val step2Df = dfIceberg1.withColumn("processing_timestamp", current_timestamp())
                            .withColumn("processing_step", lit("pipeline_5_v2_step_2"))
    val table2 = "pipeline5_step2_v2"
    val table2Name = s"`$catalogName`.`$namespace`.$table2"
    println(s"Step 2: Writing to $table2Name")
    spark.sql(s"CREATE TABLE IF NOT EXISTS $table2Name (${step2Df.schema.toDDL}) USING iceberg")
    step2Df.writeTo(table2Name).append()

    println("Compute finished. Emitting explicit BigQuery lineage via custom Lineage API...")

    // 5. Emit Custom Lineage Events
    try {
      // Define Source FQN (GCS)
      val cleanSourceGcs = if (sourceGcs.endsWith("/")) sourceGcs.substring(0, sourceGcs.length - 1) else sourceGcs
      val sourceId = cleanSourceGcs.replace("gs://", "").replace("/", ".")
      val sourceFqn = s"gcs:$sourceId"

      // Define Target FQNs (BigQuery standard format)
      val bqTarget1Fqn = s"bigquery:$projectId.$namespace.$table1"
      val bqTarget2Fqn = s"bigquery:$projectId.$namespace.$table2"

      val runName = setupLineageRun(projectId)
      
      // Emit Table-Level Lineage
      emitLineageEdge(projectId, runName, sourceFqn, bqTarget1Fqn)
      emitLineageEdge(projectId, runName, bqTarget1Fqn, bqTarget2Fqn)

      // Emit Column-Level Lineage 
      // Mapping pipeline1.pipeline_id -> pipeline2.processing_step
      val colTarget1 = s"$bqTarget1Fqn.pipeline_id"
      val colTarget2 = s"$bqTarget2Fqn.processing_step"
      emitLineageEdge(projectId, runName, colTarget1, colTarget2)

      println("Lineage emission successful (including Column-Level).")
    } catch {
      case e: Exception =>
        println(s"Failed to emit lineage: ${e.getMessage}")
        e.printStackTrace()
    }

    spark.stop()
    println("Pipeline 5 V2: Completed Successfully.")
  }

  // --- REST API Helpers ---
  def setupLineageRun(projectId: String): String = {
    val location = "us-east1"
    val token = getAccessToken()
    val processEndpoint = s"https://datalineage.googleapis.com/v1/projects/$projectId/locations/$location/processes"
    
    val processPayload = s"""{
      |  "displayName": "Pipeline5_V2_BigLake_Job",
      |  "attributes": {"custom_job_id": "pipeline5_v2"},
      |  "origin": { "sourceType": "CUSTOM", "name": "projects/$projectId/locations/$location" }
      |}""".stripMargin

    val processName = extractName(postRestApi(processEndpoint, processPayload, token, projectId))
    val runEndpoint = s"https://datalineage.googleapis.com/v1/$processName/runs"
    val now = Instant.now().toString
    val runPayload = s"""{"displayName": "Pipeline 5 V2 Run", "startTime": "$now", "endTime": "$now", "state": "COMPLETED"}"""
    
    extractName(postRestApi(runEndpoint, runPayload, token, projectId))
  }

  def emitLineageEdge(projectId: String, runName: String, sourceFqn: String, targetFqn: String): Unit = {
    val token = getAccessToken()
    val now = Instant.now().toString
    val lineageEventEndpoint = s"https://datalineage.googleapis.com/v1/$runName/lineageEvents"
    val lineageEventPayload = s"""{
      |  "startTime": "$now",
      |  "links": [
      |    {
      |      "source": { "fullyQualifiedName": "$sourceFqn" },
      |      "target": { "fullyQualifiedName": "$targetFqn" }
      |    }
      |  ]
      |}""".stripMargin

    postRestApi(lineageEventEndpoint, lineageEventPayload, token, projectId)
    println(s"Emitted Lineage Edge: $sourceFqn -> $targetFqn")
  }

  def getAccessToken(): String = {
    val tokenUrl = new URL("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token")
    val tokenConn = tokenUrl.openConnection().asInstanceOf[HttpURLConnection]
    tokenConn.setRequestProperty("Metadata-Flavor", "Google")
    val tokenReader = new BufferedReader(new InputStreamReader(tokenConn.getInputStream()))
    val tokenResponse = Iterator.continually(tokenReader.readLine()).takeWhile(_ != null).mkString("\n")
    tokenReader.close()

    val tokenPattern = Pattern.compile("\"access_token\"\\s*:\\s*\"([^\"]+)\"")
    val tokenMatcher = tokenPattern.matcher(tokenResponse)
    if (tokenMatcher.find()) tokenMatcher.group(1) else throw new RuntimeException("Could not parse access token.")
  }

  def postRestApi(endpoint: String, payload: String, token: String, projectId: String): String = {
    val url = new URL(endpoint)
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("POST")
    conn.setRequestProperty("Authorization", s"Bearer $token")
    conn.setRequestProperty("x-goog-user-project", projectId)
    conn.setRequestProperty("Content-Type", "application/json; charset=utf-8")
    conn.setDoOutput(true)

    val out = new OutputStreamWriter(conn.getOutputStream())
    out.write(payload)
    out.close()

    val responseCode = conn.getResponseCode()
    val is = if (responseCode >= 200 && responseCode < 300) conn.getInputStream() else conn.getErrorStream()
    val reader = new BufferedReader(new InputStreamReader(is))
    val response = Iterator.continually(reader.readLine()).takeWhile(_ != null).mkString("\n")
    reader.close()

    if (responseCode >= 200 && responseCode < 300) response
    else throw new RuntimeException(s"API Call Failed with code $responseCode: $response")
  }

  def extractName(json: String): String = {
    val pattern = Pattern.compile("\"name\"\\s*:\\s*\"([^\"]+)\"")
    val matcher = pattern.matcher(json)
    if (matcher.find()) matcher.group(1) else throw new RuntimeException(s"Could not parse name from JSON: $json")
  }
}
