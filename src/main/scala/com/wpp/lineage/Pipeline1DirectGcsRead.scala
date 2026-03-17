package com.wpp.lineage

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.util.UUID
import java.time.Instant

object Pipeline1DirectGcsRead {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Pipeline1_DirectGcsRead_BQ_External")
      .getOrCreate()

    if (args.length < 2) {
      System.err.println("Usage: Pipeline1DirectGcsRead <projectId> <bucketBqExternal>")
      System.exit(1)
    }
    
    val projectId = args(0)
    val bucketBqExternal = args(1)

    // Read DIRECTLY from GCS
    val sourcePath = s"gs://$bucketBqExternal/source/"
    val destPath = s"gs://$bucketBqExternal/dest/"
    
    val df = spark.read.parquet(sourcePath)

    // Basic Transformation
    val transformedDf = df.withColumn("processed_timestamp", current_timestamp())
                          .withColumn("pipeline_id", lit("pipeline_1_direct_gcs_read"))

    // Write DIRECTLY to GCS.
    // Dataproc OpenLineage will automatically detect the V2 Parquet writer.

    transformedDf.write
      .mode("overwrite")
      .parquet(destPath)

    spark.stop()
  }
}
