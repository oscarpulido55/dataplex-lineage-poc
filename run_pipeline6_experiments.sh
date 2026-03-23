#!/bin/bash

# Configuration
if [ -z "$PROJECT_ID" ]; then
    echo "Please export PROJECT_ID before running."
    exit 1
fi

if [ -z "$REGION" ]; then
    export REGION="us-central1" # default
fi

if [ -z "$SUBNET" ]; then
    echo "Please export SUBNET before running (e.g. export SUBNET=default)"
    exit 1
fi

BUCKET_MAIN="${PROJECT_ID}-demo_lineage_poc"

echo "============================================================"
echo " PIpeline 6: Uniqueness Experiments (Dataproc Serverless) "
echo "============================================================"
echo "Note: Make sure you have run 'terraform apply' to create the Pipeline 6 datasets."
echo "Make sure you have re-compiled the jar: sbt clean assembly && gcloud storage cp target/scala-2.13/lineage-poc-assembly-1.0.jar gs://$BUCKET_MAIN/jars/"


echo -e "\n\n---> PHASE 1: Baseline Run"
RUN_ID_1=$(date +%s)
gcloud dataproc batches submit spark \
    --project=$PROJECT_ID \
    --region=$REGION \
    --batch="pipeline6-baseline-$RUN_ID_1" \
    --version=3.0 \
    --class=com.demo.lineage.Pipeline6LineageUniqueness \
    --jars="gs://${BUCKET_MAIN}/jars/lineage-poc-assembly-1.0.jar" \
    --subnet=$SUBNET \
    --properties="spark.dataproc.lineage.enabled=true" \
    -- $PROJECT_ID "_baseline"
echo "Phase 1 batch submitted. ID: pipeline6-baseline-$RUN_ID_1"


echo -e "\n\n---> PHASE 2: Same appName, New Batch UUID"
RUN_ID_2=$(date +%s)
gcloud dataproc batches submit spark \
    --project=$PROJECT_ID \
    --region=$REGION \
    --batch="pipeline6-baseline-$RUN_ID_2" \
    --version=3.0 \
    --class=com.demo.lineage.Pipeline6LineageUniqueness \
    --jars="gs://${BUCKET_MAIN}/jars/lineage-poc-assembly-1.0.jar" \
    --subnet=$SUBNET \
    --properties="spark.dataproc.lineage.enabled=true" \
    -- $PROJECT_ID "_baseline"
echo "Phase 2 batch submitted. ID: pipeline6-baseline-$RUN_ID_2"


echo -e "\n\n---> PHASE 3: Change appName (overriding via spark properties)"
RUN_ID_3=$(date +%s)
gcloud dataproc batches submit spark \
    --project=$PROJECT_ID \
    --region=$REGION \
    --batch="pipeline6-v2-$RUN_ID_3" \
    --version=3.0 \
    --class=com.demo.lineage.Pipeline6LineageUniqueness \
    --jars="gs://${BUCKET_MAIN}/jars/lineage-poc-assembly-1.0.jar" \
    --subnet=$SUBNET \
    --properties="spark.dataproc.lineage.enabled=true,spark.openlineage.appName=Pipeline6_Uniqueness_V2" \
    -- $PROJECT_ID "_v2"
echo "Phase 3 batch submitted. ID: pipeline6-v2-$RUN_ID_3"


echo -e "\n\n---> PHASE 4: Explicit OpenLineage Configs (changing namespace)"
RUN_ID_4=$(date +%s)
gcloud dataproc batches submit spark \
    --project=$PROJECT_ID \
    --region=$REGION \
    --batch="pipeline6-explicit-$RUN_ID_4" \
    --version=3.0 \
    --class=com.demo.lineage.Pipeline6LineageUniqueness \
    --jars="gs://${BUCKET_MAIN}/jars/lineage-poc-assembly-1.0.jar" \
    --subnet=$SUBNET \
    --properties="spark.dataproc.lineage.enabled=true,spark.openlineage.namespace=custom_namespace,spark.openlineage.appName=Pipeline6_Explicit" \
    -- $PROJECT_ID "_explicit"
echo "Phase 4 batch submitted. ID: pipeline6-explicit-$RUN_ID_4"

echo -e "\n\nAll batches submitted. Please wait a few minutes for Dataplex Lineage API to ingest the events."
echo "Then, run the inspection script:"
echo "  python3 inspect_lineage_by_table.py bigquery:$PROJECT_ID.demo_lineage_poc_pipeline6_native.pipeline6_source"
