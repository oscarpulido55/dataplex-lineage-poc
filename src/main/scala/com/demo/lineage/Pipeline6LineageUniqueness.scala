package com.demo.lineage

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Pipeline6LineageUniqueness {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: Pipeline6LineageUniqueness <projectId> <phaseOrDestTable>")
      System.exit(1)
    }

    val projectId = args(0)
    val destSuffix = args(1) // Allows testing Phase 4

    // The appName can also be overridden by spark-submit params if needed,
    // but we default to Pipeline6_Uniqueness for Phases 1 & 2.
    // If we want to change it for Phase 3, we can change the source code or use spark-submit.
    val spark = SparkSession.builder()
      .appName("Pipeline6_Uniqueness") 
      .getOrCreate()

    println("Pipeline 6: Starting Spark job for lineage uniqueness experiments...")

    // Read using the BigQuery Spark Connector targeting the native table
    val sourceTable = s"$projectId.demo_lineage_poc_pipeline6_native.pipeline6_source"
    println(s"Reading data from native BQ: $sourceTable")
    
    val df = spark.read.format("bigquery")
      .option("table", sourceTable)
      .load()

    // Simple Transformation
    println("Performing transformation strategy...")
    val transformedDf = df
      .withColumn("processed_timestamp", current_timestamp())
      .withColumn("pipeline_id", lit("pipeline_6_uniqueness_test"))

    // Write back directly to a new native destination table in the same dataset
    val destTable = s"$projectId.demo_lineage_poc_pipeline6_native.pipeline6_dest${destSuffix}"
    println(s"Writing data to native BQ: $destTable")
    
    transformedDf.write.format("bigquery")
      .option("table", destTable)
      .option("temporaryGcsBucket", s"$projectId-demo_lineage_poc_bq_native")
      .mode("overwrite")
      .save()

    println("Pipeline 6: Completed Successfully.")
    spark.stop()
  }
}
