#!/bin/bash

# Configuration
# Setup variables
PROJECT_ID="<YOUR_PROJECT_ID_HERE>"
REGION="us-east1"
SUBNET="prod-vpc-us-east1"

# Automatically generate a unique suffix for batch names to allow multiple runs
RUN_ID=$(date +%s)

echo "Submitting Pipeline 0: Pure BQ-to-BQ Native..."
gcloud dataproc batches submit spark \
    --project=$PROJECT_ID \
    --region=$REGION \
    --batch="lineage-pipeline0-native-$RUN_ID" \
    --version=3.0 \
    --class=com.demo.lineage.Pipeline0BQNative \
    --jars="gs://${PROJECT_ID}-demo_lineage_poc/jars/lineage-poc-assembly-1.0.jar" \
    --subnet=$SUBNET \
    --properties="spark.dataproc.lineage.enabled=true" \
    -- $PROJECT_ID

echo "Submitting Pipeline 1: Pure GCS-to-GCS (Direct File Operations)..."
gcloud dataproc batches submit spark \
    --project=$PROJECT_ID \
    --region=$REGION \
    --batch="lineage-direct-gcs-$RUN_ID" \
    --version=3.0 \
    --class=com.demo.lineage.Pipeline1DirectGcsRead \
    --jars="gs://${PROJECT_ID}-demo_lineage_poc/jars/lineage-poc-assembly-1.0.jar" \
    --subnet=$SUBNET \
    --properties="spark.dataproc.lineage.enabled=true" \
    -- $PROJECT_ID "${PROJECT_ID}-demo_lineage_poc_direct_gcs"

echo "Submitting Pipeline 2: BigQuery Connector via BigLake (External Tables)..."
gcloud dataproc batches submit spark \
    --project=$PROJECT_ID \
    --region=$REGION \
    --batch="lineage-biglake-$RUN_ID" \
    --version=3.0 \
    --class=com.demo.lineage.Pipeline2BQExternal \
    --jars="gs://${PROJECT_ID}-demo_lineage_poc/jars/lineage-poc-assembly-1.0.jar" \
    --subnet=$SUBNET \
    --properties="spark.dataproc.lineage.enabled=true" \
    -- $PROJECT_ID "${PROJECT_ID}-demo_lineage_poc_bq_external"

echo "Submitting Pipeline 3: Native Dataplex Path..."
gcloud dataproc batches submit spark \
    --project=$PROJECT_ID \
    --region=$REGION \
    --batch="lineage-native-dataplex-$RUN_ID" \
    --version=3.0 \
    --class=com.demo.lineage.Pipeline3NativeDataplex \
    --jars="gs://${PROJECT_ID}-demo_lineage_poc/jars/lineage-poc-assembly-1.0.jar" \
    --subnet=$SUBNET \
    --properties="spark.dataproc.lineage.enabled=true" \
    -- $PROJECT_ID "${PROJECT_ID}-demo_lineage_poc_auto_discovery"

echo "Submitting Pipeline 4: Custom REST API Lineage..."
gcloud dataproc batches submit spark \
    --project=$PROJECT_ID \
    --region=$REGION \
    --batch="lineage-custom-api-$RUN_ID" \
    --version=3.0 \
    --class=com.demo.lineage.Pipeline4CustomLineage \
    --jars="gs://${PROJECT_ID}-demo_lineage_poc/jars/lineage-poc-assembly-1.0.jar" \
    --subnet=$SUBNET \
    -- "${PROJECT_ID}-demo_lineage_poc_lineage_api"

echo "Submitting Pipeline 5 V2: Custom BigLake Rest Catalog..."
CATALOG_NAME_V2=$(echo "${PROJECT_ID}_demo_lineage_poc_blms_catalog_v2" | tr '-' '_')

gcloud dataproc batches submit spark \
    --project=$PROJECT_ID \
    --region=$REGION \
    --batch="lineage-blms-custom-$RUN_ID" \
    --version=3.0 \
    --class=com.demo.lineage.Pipeline5CustomBigLake \
    --jars="gs://${PROJECT_ID}-demo_lineage_poc/jars/lineage-poc-assembly-1.0.jar" \
    --subnet=$SUBNET \
    --properties="spark.sql.defaultCatalog=$CATALOG_NAME_V2,\
spark.sql.catalog.$CATALOG_NAME_V2=org.apache.iceberg.spark.SparkCatalog,\
spark.sql.catalog.$CATALOG_NAME_V2.type=rest,\
spark.sql.catalog.$CATALOG_NAME_V2.uri=https://biglake.googleapis.com/iceberg/v1/restcatalog,\
spark.sql.catalog.$CATALOG_NAME_V2.warehouse=bq://projects/${PROJECT_ID},\
spark.sql.catalog.$CATALOG_NAME_V2.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO,\
spark.sql.catalog.$CATALOG_NAME_V2.header.x-goog-user-project=${PROJECT_ID},\
spark.sql.catalog.$CATALOG_NAME_V2.rest.auth.type=org.apache.iceberg.gcp.auth.GoogleAuthManager,\
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,\
spark.sql.catalog.$CATALOG_NAME_V2.rest-metrics-reporting-enabled=false" \
    -- $PROJECT_ID "${PROJECT_ID}-demo_lineage_poc_blms_data_v2" "$CATALOG_NAME_V2" "$REGION"

echo "All batches submitted successfully with run ID: $RUN_ID"
