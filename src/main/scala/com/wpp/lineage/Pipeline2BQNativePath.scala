package com.wpp.lineage

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Pipeline2BQNativePath {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: Pipeline2BQNativePath <projectId> <bucketMain>")
      System.exit(1)
    }

    val projectId = args(0)
    val bucketMain = args(1)

    // 1. Initialize Spark Session built-in with Dataproc OpenLineage properties
    val spark = SparkSession.builder()
      .appName("Pipeline2_BQ_Native")
      .getOrCreate()

    println("Pipeline 2: Starting BQ to BQ external tables direct Native interaction...")

    // 2. Read using the BigQuery Spark Connector. 
    val sourceTable = s"$projectId.wpp_lineage_poc_bq_native.native_bq_tables_source"
    println(s"Reading data from: $sourceTable")
    val df = spark.read.format("bigquery")
      .load(sourceTable)

    // 3. Simple Transformation
    println("Performing transformation strategy...")
    val transformedDf = df
      .withColumn("processed_timestamp", current_timestamp())
      .withColumn("pipeline_id", lit("pipeline_2_bq_native"))

    // 4. Write back directly to the GCS path acting as the External Table Destination.
    // The BigQuery Connector API does not support writing to External Tables.
    val destPath = s"gs://$projectId-wpp_lineage_poc_bq_native/dest/"
    println(s"Writing data to GCS: $destPath")
    
    transformedDf.write
      .mode("overwrite")
      .parquet(destPath)

    println("Pipeline 2: Completed Successfully.")
    spark.stop()
  }
}
