package com.demo.lineage

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Pipeline0BQNative {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: Pipeline0BQNative <projectId>")
      System.exit(1)
    }

    val projectId = args(0)

    // 1. Initialize Spark Session built-in with Dataproc OpenLineage properties
    val spark = SparkSession.builder()
      .appName("Pipeline0_BQ_Pure_Native")
      .getOrCreate()

    println("Pipeline 0: Starting Pure BQ-to-BQ Native interaction...")

    // 2. Read using the BigQuery Spark Connector targeting the native table
    val sourceTable = s"$projectId.demo_lineage_poc_pipeline0_native.pure_native_source"
    println(s"Reading data from native BQ: $sourceTable")
    
    val df = spark.read.format("bigquery")
      .option("table", sourceTable)
      .load()

    // 3. Simple Transformation
    println("Performing transformation strategy...")
    val transformedDf = df
      .withColumn("processed_timestamp", current_timestamp())
      .withColumn("pipeline_id", lit("pipeline_0_pure_native"))

    // 4. Write back directly to a new native destination table in the same dataset
    val destTable = s"$projectId.demo_lineage_poc_pipeline0_native.pure_native_dest"
    println(s"Writing data to native BQ: $destTable")
    
    transformedDf.write.format("bigquery")
      .option("table", destTable)
      .option("temporaryGcsBucket", s"$projectId-demo_lineage_poc_bq_native")
      .mode("overwrite")
      .save()

    println("Pipeline 0: Completed Successfully.")
    spark.stop()
  }
}
