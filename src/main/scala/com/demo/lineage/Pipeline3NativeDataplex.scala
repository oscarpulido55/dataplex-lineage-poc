package com.demo.lineage

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.util.UUID
import java.time.Instant

object Pipeline3NativeDataplex {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Pipeline3_NativeDataplex")
      .getOrCreate()

    if (args.length < 2) {
      System.err.println("Usage: Pipeline3NativeDataplex <projectId> <bucketAutoDiscovery>")
      System.exit(1)
    }
    
    val projectId = args(0)
    val bucketAutoDiscovery = args(1)

    // Read DIRECTLY from GCS
    val sourcePath = s"gs://$bucketAutoDiscovery/source/"
    val destPath = s"gs://$bucketAutoDiscovery/dest/"
    
    val df = spark.read.parquet(sourcePath)

    // Basic Transformation
    val transformedDf = df.withColumn("processed_timestamp", current_timestamp())
                          .withColumn("pipeline_id", lit("pipeline_3_native_dataplex"))

    // Write DIRECTLY to GCS.
    // Dataproc OpenLineage will automatically detect the V2 Parquet writer.

    transformedDf.write
      .mode("overwrite")
      .parquet(destPath)

    spark.stop()
  }
}
