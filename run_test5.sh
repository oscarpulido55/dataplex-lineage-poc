#!/bin/bash
export PROJECT_ID="<YOUR_PROJECT_ID_HERE>"
export REGION="us-east1"
export SUBNET="prod-vpc-us-east1"
export RUN_ID=$(date +%Y%m%d%H%M%S)
CATALOG_NAME_V2=$(echo "${PROJECT_ID}_demo_lineage_poc_blms_catalog_v2" | tr '-' '_')

echo "Submitting Pipeline 5 V2: Custom BigLake Rest Catalog Lineage Injection..."

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
