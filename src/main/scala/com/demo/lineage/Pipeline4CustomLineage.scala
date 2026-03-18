package com.demo.lineage

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Pipeline4CustomLineage {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: Pipeline4CustomLineage <bucketApiLineage>")
      System.exit(1)
    }

    val bucketApiLineage = args(0)
    val projectId = sys.env.getOrElse("GOOGLE_CLOUD_PROJECT", "<YOUR_PROJECT_ID_HERE>")
    val location = "us-east1"

    // 1. Define Paths
    val sourceGcs = s"gs://$bucketApiLineage/source/"
    val destGcs = s"gs://$bucketApiLineage/dest/"

    println("Pipeline 4: Starting GCS to GCS Transformation (Manual Custom API Path)...")

    val spark = SparkSession.builder()
      .appName("Pipeline 4: Custom API Native Lineage")
      .getOrCreate()

    // 2. Compute (Spark)
    println(s"Reading data from: $sourceGcs")
    val df = spark.read.parquet(sourceGcs)

    println("Performing transformation strategy...")
    val transformedDf = df
      .withColumn("processed_timestamp", current_timestamp())
      .withColumn("pipeline_id", lit("pipeline_4_api_managed"))

    println(s"Writing data to: $destGcs")
    transformedDf.write
      .mode("overwrite")
      .parquet(destGcs)

    // 3. Emit Custom Lineage Manually
    println("Compute finished. Emitting explicit lineage via Dataplex Lineage API...")
    try {
      emitCustomLineage(projectId, location, sourceGcs, destGcs)
      println("Lineage emission successful.")
    } catch {
      case e: Exception =>
        println(s"Failed to emit lineage: ${e.getMessage}")
        e.printStackTrace()
    }

    println("Pipeline 4: Completed Successfully.")
    spark.stop()
  }

  def emitCustomLineage(projectId: String, location: String, sourceGcs: String, destGcs: String): Unit = {
    import java.net.HttpURLConnection
    import java.net.URL
    import java.io.{BufferedReader, InputStreamReader, OutputStreamWriter}
    import java.time.Instant

    // 1. Get Access Token via Metadata Server
    println("Fetching access token from Metadata server...")
    val tokenUrl = new URL("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token")
    val tokenConn = tokenUrl.openConnection().asInstanceOf[HttpURLConnection]
    tokenConn.setRequestProperty("Metadata-Flavor", "Google")
    val tokenReader = new BufferedReader(new InputStreamReader(tokenConn.getInputStream()))
    val tokenResponse = Iterator.continually(tokenReader.readLine()).takeWhile(_ != null).mkString("\n")
    tokenReader.close()

    import java.util.regex.Pattern

    val tokenPattern = Pattern.compile("\"access_token\"\\s*:\\s*\"([^\"]+)\"")
    val tokenMatcher = tokenPattern.matcher(tokenResponse)
    val token = if (tokenMatcher.find()) tokenMatcher.group(1) else throw new RuntimeException("Could not parse access token.")
    println("Access token acquired.")

    // Helper for making REST API calls
    def postRestApi(endpoint: String, payload: String): String = {
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

      if (responseCode >= 200 && responseCode < 300) {
        response
      } else {
        throw new RuntimeException(s"API Call Failed with code $responseCode: $response")
      }
    }

    // Helper for making GET REST API calls to check existence
    def getRestApi(endpoint: String): Option[String] = {
      val url = new URL(endpoint)
      val conn = url.openConnection().asInstanceOf[HttpURLConnection]
      conn.setRequestMethod("GET")
      conn.setRequestProperty("Authorization", s"Bearer $token")
      conn.setRequestProperty("x-goog-user-project", projectId)
      
      val responseCode = conn.getResponseCode()
      if (responseCode >= 200 && responseCode < 300) {
        val reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))
        val response = Iterator.continually(reader.readLine()).takeWhile(_ != null).mkString("\n")
        reader.close()
        Some(response)
      } else if (responseCode == 404) {
        None
      } else {
        val reader = new BufferedReader(new InputStreamReader(conn.getErrorStream()))
        val errorResponse = Iterator.continually(reader.readLine()).takeWhile(_ != null).mkString("\n")
        reader.close()
        throw new RuntimeException(s"GET API Call Failed with code $responseCode: $errorResponse")
      }
    }

    // Extract 'name' field from JSON response using pure Java regex to avoid Scala version linkage conflicts
    def extractName(json: String): String = {
      val pattern = java.util.regex.Pattern.compile("\"name\"\\s*:\\s*\"([^\"]+)\"")
      val matcher = pattern.matcher(json)
      if (matcher.find()) matcher.group(1) else throw new RuntimeException(s"Could not parse name from JSON: $json")
    }

    val processEndpoint = s"https://datalineage.googleapis.com/v1/projects/$projectId/locations/$location/processes"
    
    // 2. Ensure Dataplex Entries Exist
    val entryGroup = "demo-lineage-poc-lineage-api-group"
    val entriesEndpoint = s"https://dataplex.googleapis.com/v1/projects/$projectId/locations/$location/entryGroups/$entryGroup/entries"
    
    def ensureEntryExists(entryId: String, gcsPath: String, displayName: String): Unit = {
      val getEndpoint = s"$entriesEndpoint/$entryId"
      println(s"Checking if Dataplex Entry exists: $entryId")
      val existingEntry = getRestApi(getEndpoint)
      
      if (existingEntry.isEmpty) {
        println(s"Entry $entryId not found. Creating...")
        val postEndpoint = s"$entriesEndpoint?entryId=$entryId"
        // Strip the trailing slash for the system resource
        val cleanPath = if (gcsPath.endsWith("/")) gcsPath.substring(0, gcsPath.length - 1) else gcsPath
        // Format to //storage.googleapis.com
        val systemResource = cleanPath.replace("gs://", "//storage.googleapis.com/")
        // Format to custom:demo_lineage_poc.source
        val customId = cleanPath.replace("gs://", "").replace("/", ".")
        val gcsFqn = s"custom:demo_lineage_poc.$customId"
        
        val payload = s"""{
          |  "entryType": "projects/$projectId/locations/$location/entryTypes/demo-lineage-poc-custom-type",
          |  "fullyQualifiedName": "$gcsFqn",
          |  "entrySource": {
          |    "resource": "$systemResource",
          |    "system": "cloud_storage",
          |    "displayName": "$displayName"
          |  },
          |  "aspects": {
          |    "$projectId.$location.demo-lineage-poc-custom-aspect": {
          |      "aspectType": "projects/$projectId/locations/$location/aspectTypes/demo-lineage-poc-custom-aspect",
          |      "data": {
          |        "format": "parquet",
          |        "framework": "TYDA",
          |        "path": "$gcsPath"
          |      }
          |    }
          |  }
          |}""".stripMargin
          
        postRestApi(postEndpoint, payload)
        println(s"Successfully created Dataplex Entry: $entryId")
      } else {
        println(s"Dataplex Entry $entryId already exists.")
      }
    }

    ensureEntryExists("pipeline4-source", sourceGcs, "Pipeline 4 Source")
    ensureEntryExists("pipeline4-dest", destGcs, "Pipeline 4 Destination")

    // 3. Create Process
    println(s"Creating Process...")
    val processPayload =
      s"""{
         |  "displayName": "LineagePOC_Manual_Spark_Job",
         |  "attributes": {"custom_job_id": "job_pipeline_4"},
         |  "origin": {
         |    "sourceType": "CUSTOM",
         |    "name": "projects/$projectId/locations/$location"
         |  }
         |}""".stripMargin

    val processResponse = postRestApi(processEndpoint, processPayload)
    val processName = extractName(processResponse)
    println(s"Created Process: $processName")

    // 3. Create Run
    println(s"Creating Run...")
    val now = Instant.now().toString
    val runEndpoint = s"https://datalineage.googleapis.com/v1/$processName/runs"
    val runPayload =
      s"""{
         |  "displayName": "Pipeline 4 Execution Run",
         |  "startTime": "$now",
         |  "endTime": "$now",
         |  "state": "COMPLETED"
         |}""".stripMargin

    val runResponse = postRestApi(runEndpoint, runPayload)
    val runName = extractName(runResponse)
    println(s"Created Run: $runName")

    // 5. Create Lineage Event
    println(s"Creating Lineage Event...")
    val cleanSourceGcs = if (sourceGcs.endsWith("/")) sourceGcs.substring(0, sourceGcs.length - 1) else sourceGcs
    val cleanDestGcs = if (destGcs.endsWith("/")) destGcs.substring(0, destGcs.length - 1) else destGcs
    
    val sourceCustomId = cleanSourceGcs.replace("gs://", "").replace("/", ".")
    val destCustomId = cleanDestGcs.replace("gs://", "").replace("/", ".")
    
    val sourceFqn = s"custom:demo_lineage_poc.$sourceCustomId"
    val destFqn = s"custom:demo_lineage_poc.$destCustomId"
    
    val lineageEventEndpoint = s"https://datalineage.googleapis.com/v1/$runName/lineageEvents"
    val lineageEventPayload = s"""{
      |  "startTime": "$now",
      |  "links": [
      |    {
      |      "source": { "fullyQualifiedName": "$sourceFqn" },
      |      "target": { "fullyQualifiedName": "$destFqn" }
      |    }
      |  ]
      |}""".stripMargin

    postRestApi(lineageEventEndpoint, lineageEventPayload)
    println("Created Lineage Event successfully.")
  }
}

