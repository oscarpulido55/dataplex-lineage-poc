#!/bin/bash

# Configuration
PROJECT_ID="YOUR_PROJECT_ID"
REGION="us-east1"
SUBNET="YOUR_SUBNET"

# Automatically generate a unique suffix for batch names to allow multiple runs
RUN_ID=$(date +%s)

echo "Submitting Pipeline 1: BQ External Tables (Direct GCS Read)..."
gcloud dataproc batches submit spark \
    --project=$PROJECT_ID \
    --region=$REGION \
    --batch="lineage-direct-gcs-$RUN_ID" \
    --class=com.wpp.lineage.Pipeline1DirectGcsRead \
    --jars="gs://${PROJECT_ID}-wpp_lineage_poc/jars/lineage-poc-assembly-1.0.jar" \
    --subnet=$SUBNET \
    --properties="spark.dataproc.lineage.enabled=true" \
    -- $PROJECT_ID "${PROJECT_ID}-wpp_lineage_poc_bq_external"

echo "Submitting Pipeline 2: BigQuery Connector via BigLake..."
gcloud dataproc batches submit spark \
    --project=$PROJECT_ID \
    --region=$REGION \
    --batch="lineage-biglake-$RUN_ID" \
    --class=com.wpp.lineage.Pipeline2BQNativePath \
    --jars="gs://${PROJECT_ID}-wpp_lineage_poc/jars/lineage-poc-assembly-1.0.jar" \
    --subnet=$SUBNET \
    --properties="spark.dataproc.lineage.enabled=true" \
    -- $PROJECT_ID "${PROJECT_ID}-wpp_lineage_poc_bq_native"

echo "Submitting Pipeline 3: Native Dataplex Path..."
gcloud dataproc batches submit spark \
    --project=$PROJECT_ID \
    --region=$REGION \
    --batch="lineage-native-dataplex-$RUN_ID" \
    --class=com.wpp.lineage.Pipeline3NativeDataplex \
    --jars="gs://${PROJECT_ID}-wpp_lineage_poc/jars/lineage-poc-assembly-1.0.jar" \
    --subnet=$SUBNET \
    --properties="spark.dataproc.lineage.enabled=true" \
    -- $PROJECT_ID "${PROJECT_ID}-wpp_lineage_poc_auto_discovery"

echo "Submitting Pipeline 4: Custom REST API Lineage..."
gcloud dataproc batches submit spark \
    --project=$PROJECT_ID \
    --region=$REGION \
    --batch="lineage-custom-api-$RUN_ID" \
    --class=com.wpp.lineage.Pipeline4CustomLineage \
    --jars="gs://${PROJECT_ID}-wpp_lineage_poc/jars/lineage-poc-assembly-1.0.jar" \
    --subnet=$SUBNET \
    -- "${PROJECT_ID}-wpp_lineage_poc_lineage_api"

echo "All batches submitted successfully with run ID: $RUN_ID"
