# Demo Dataplex Lineage POC

This project implements three distinct mechanisms for orchestrating Data Lineage on Google Cloud Dataproc Serverless, integrated directly with the Dataplex Universal Catalog.

## Infrastructure Setup
GCP Infrastructure is fully managed via Terraform:
```bash
cd terraform
terraform init
terraform plan -var="project_id=<YOUR_PROJECT_ID_HERE>"
terraform apply -var="project_id=<YOUR_PROJECT_ID_HERE>"
```
This provisions:
- BigQuery Datasets & External Tables (For Pipeline 1)
- BigLake Connection & BigQuery Views (For Pipeline 2)
- Dataplex Data Scan & Entry Group (For Pipeline 3)
- Dataplex Entry Group anchor (For Pipeline 4)

## Setting Up Environment Variables
Before running the pipelines, export your target infrastructure settings:
```bash
export PROJECT_ID="<YOUR_PROJECT_ID_HERE>"
export REGION="<YOUR_REGION_HERE>"
export SUBNET="<YOUR_SUBNET_HERE>"
export BUCKET_MAIN="${PROJECT_ID}-demo_lineage_poc"
export BUCKET_DIRECT_GCS="${PROJECT_ID}-demo_lineage_poc_direct_gcs"
export BUCKET_DISCOVERY="${PROJECT_ID}-demo_lineage_poc_auto_discovery"
export BUCKET_API="${PROJECT_ID}-demo_lineage_poc_lineage_api"
export BUCKET_BQ_EXTERNAL="${PROJECT_ID}-demo_lineage_poc_bq_external"
```

## Compiling the Spark Job
Because Dataproc natively bundles `protobuf`, `grpc`, and the Spark BigQuery connector, these dependencies are marked as `% "provided"` to prevent JVM "Classpath Hell".
```bash
sbt clean assembly
gcloud storage cp target/scala-2.13/lineage-poc-assembly-1.0.jar gs://$BUCKET_MAIN/jars/lineage-poc-assembly-1.0.jar --project=$PROJECT_ID
```

## Running the Pipelines


All pipelines utilize Dataproc Serverless to stitch Spark operations into the Dataplex Universal Catalog.

### Key Architectural Findings & Column-Level Lineage

#### 0. Pipeline 0: Pure BQ-to-BQ Native
* **Prerequisites**: A native BigQuery dataset with a native table populated with data,. No GCS interactions in Spark code.
* **Compute Engine**: **Shared (BigQuery + Dataproc)**. The `spark-bigquery` connector utilizes the BigQuery Storage Read/Write API.
* **Column-Level Lineage**: **Supported**. Standard BigQuery Storage API read hooks natively broadcast the exact schemas and Table FQNs directly to OpenLineage for both reading and writing.
* **Pros**: Operates strictly within the native GCP BigQuery execution framework, providing full table and column-level lineage visibility.
* **Cons**: Incurs BigQuery API costs.
* **Validation**:
  ```bash
  export PROJECT_ID="<YOUR_PROJECT_ID_HERE>"
  python inspect_batch_lineage.py lineage-pipeline0-native
  ```

#### 1. Pipeline 1: Pure GCS-to-GCS (Direct File Operations)
* **Prerequisites**: Just standard Google Cloud Storage Bucket(s). No BigQuery external tables are utilized or needed here.
* **Compute Engine**: **100% Dataproc**. Because the job reads and writes raw GCS parquet files directly (`spark.read.parquet` & `df.write.parquet`), it bypasses the BigQuery execution engine entirely. All processing happens in Spark.
* **Lineage Support**: **Captured but Visually Hidden**. The Dataproc OpenLineage agent organically detects and outputs lineage events under the raw `gcs:` Fully Qualified Name (FQN). While these lineage edges *do exist* in the Dataplex backend, they cannot be seen inside the Google Cloud / Dataplex UI because the raw `gcs:` path does not map to a recognized "Managed Asset" (like a Data Scan or BigQuery Table). 
* **Validation**: Run the included `inspect_batch_lineage.py` tool to extract the hidden raw lineage edges from the API:
  ```bash
  export PROJECT_ID="<YOUR_PROJECT_ID_HERE>"
  python inspect_batch_lineage.py lineage-direct-gcs-verify
  ```
* **Pros**: Zero BigQuery API costs. Pure Spark execution. True serverless tracking via OpenLineage.
* **Cons**: Fails to render natively inside the Dataplex visual graph, and cannot support column-level lineage out of the box because there is no Managed Catalog Entity to attach schemas to.

#### 2. Pipeline 2: BQ Connector via BigLake
* **Prerequisites**: `google_bigquery_connection`, `google_bigquery_table` (BigLake External Table)
* **Compute Engine**: **Shared (BigQuery + Dataproc)**. The `spark-bigquery` connector utilizes the BigQuery Storage Read API. Filter and projection pushdowns are evaluated natively using BigQuery compute slots, while the remaining DataFrame transformations are processed by Dataproc Spark workers.
* **Column-Level Lineage**: **Supported**. Standard BigQuery Storage API read hooks natively broadcast the exact BigLake schema and BigQuery Table FQNs directly to OpenLineage.
* **Pros**: Operates strictly within the native GCP BigQuery execution framework, providing full table and column-level lineage visibility.
* **Cons**: Cannot perform direct writes (`spark.write.format("bigquery")`) to BigLake External tables; must fallback to raw GCS Parquet writes for the destination. Incurs BigQuery Storage API read costs compared to Pipeline 1.

#### 3. Pipeline 3: Dataplex Auto-Discovery
* **Prerequisites**: `google_dataplex_lake`, `google_dataplex_zone`, `google_dataplex_asset`, `google_dataplex_datascan` (Discovery)
  * **Why are Lakes and Zones required?**: Dataplex Auto-Discovery strictly requires its target storage (GCS buckets) to first be physically mapped into the Dataplex framework as `Assets` residing within a `Zone` and `Lake` hierarchy before the Discovery scan can crawl and register the files as catalog entries.
* **Column-Level Lineage**: **Supported** conditionally. Dataplex Data Scans automatically detect schemas for structured files (Parquet/AVRO) and propagate them to the Catalog, allowing column-level lineage mapping.
* **Pros**: Low overhead. No BigQuery integration required. Catalog entries populate dynamically based on wildcards.
* **Cons**:   
  * Potential timing issues if Spark runs before the Dataplex Discovery Scan (usually 1-hour intervals) registers the new data entry.
  * **Upgrade to Managed Limitations (No BigQuery Graph)**: Even if the tables are explicitly "upgraded to managed" within Dataplex, Dataproc OpenLineage's native integration points to the `gcs:` storage FQN (e.g., `gcs:bucket.dest`). The Lineage UI expects the `bigquery:` FQN. Because the backend does not automatically stitch `gcs:` events to `bigquery:` external tables, the visual graph in BigQuery stays empty. 
  * **Note on Pipeline 2 vs 3 Similarity**: If the `spark-bigquery` connector is used (as in Pipeline 2), OpenLineage emits the `bigquery:` FQN which renders in the UI. Using direct `gcs:` reads (as in Pipeline 1 and 3) fails to map to Native Dataplex entities out of the box, even if the destination is later manually or automatically "upgraded to managed" via Dataplex Auto-Discovery.

#### 4. Pipeline 4: Custom REST API
* **Prerequisites**: `google_dataplex_entry_group`, `google_dataplex_entry_type`, `google_dataplex_aspect_type`
  * **Why is an Entry Group required?**: Because the Auto-Discovery `Lake/Zone` hierarchy is bypassed, a purely logical `Entry Group` must be explicitly provisioned to serve as the structural anchor for manually minted `custom` Lineage Entries and their `Entry/Aspect Types`.
* **Column-Level Lineage**: **Not Supported**. The `custom:` FQN format currently maps to standard entity definitions. While custom aspects can hold arbitrary schema text, native Lineage UI visualization for columns relies on Google-managed BigQuery/Dataplex Discovery entities.
* **Pros**: Absolute control. Enforces 1:1 lineage linking via explicit FQNs (`custom:namespace.target`). Impossible to produce "ghost nodes". Supports custom metadata "Aspects" natively.
* **Cons**: High engineering cost. Requires managing custom API integration within the Spark application code. No native column-level graph.

#### 5. Pipeline 5 V2: Iceberg BigLake REST Catalog (Custom API Injection)
* **Prerequisites**: A functional Iceberg BigLake REST Catalog integration (`bq://` federation).
* **Column-Level Lineage**: **Supported (via Custom API)**. Because native OpenLineage drops OSS DML write events for unified Iceberg tables (see limitations below), this pipeline explicitly calls the `datalineage.googleapis.com` REST API natively within the Spark application to emit both table-level tracking *and* explicit column-level lineage mappings (e.g., `pipeline_id` -> `processing_step`).
* **Pros**: Overcomes native limitations of OpenLineage on Iceberg. Produces a pristine `bigquery:` FQN graph identical to Pipeline 0 within the Dataplex UI, fully validating that unified column-level tracking can be coerced natively.
* **Cons**: Requires manual backend REST manipulation for lineage emission within the core transformation logic.

## Known Limitations: Iceberg BigLake REST Catalog Lineage

Pipeline 5 outlines a unified BigLake REST Catalog integration. It highlights native limitations regarding Dataproc OpenLineage and BigLake Metastore Federation (`bq://` URIs).

As outlined in the Google Cloud PRD for "Unified BigLake Iceberg Tables," **lineage for OSS DML (e.g., Spark writes to Unified Iceberg Tables) is currently out of scope**. This manifests technically as follows:
- When writing to Iceberg using the BigLake REST Catalog configured with BigQuery Federation (e.g. `warehouse=bq://projects/...`), omitting the catalog name in Spark SQL causes OpenLineage to intercept the logical `bq://` warehouse path.
- The Dataplex Lineage API does not support `bq://` as a schema and crashes with an `INVALID_ARGUMENT: Unrecognized input` GRPC error, completely **dropping the write event**.
- Consequently, the lineage graph fails to render target nodes for these writes organically.

**Solution**: For strict logical `bigquery:` FQN lineage representation on unsupported OSS engines, users must manually self-report lineage graphs via the Lineage REST API (implemented successfully in **Pipeline 5 V2**). This provides complete, uncompromising table and column-level control.

### Execution Commands

```bash
# Pipeline 0: Pure BigQuery Native
gcloud dataproc batches submit spark \
    --project=$PROJECT_ID \
    --region=$REGION \
    --batch=lineage-pipeline0-native-$RANDOM \
    --version=3.0 \
    --class=com.demo.lineage.Pipeline0BQNative \
    --jars=gs://$BUCKET_MAIN/jars/lineage-poc-assembly-1.0.jar \
    --subnet=$SUBNET \
    --properties="spark.dataproc.lineage.enabled=true" \
    -- $PROJECT_ID

# Inspect Pipeline 0
python3 inspect_batch_lineage.py lineage-pipeline0-native-21793

# Pipeline 1: BQ External Tables (Direct GCS Read)
gcloud dataproc batches submit spark \
    --project=$PROJECT_ID \
    --region=$REGION \
    --batch=lineage-direct-gcs-$RANDOM \
    --version=3.0 \
    --class=com.demo.lineage.Pipeline1DirectGcsRead \
    --jars=gs://$BUCKET_MAIN/jars/lineage-poc-assembly-1.0.jar \
    --subnet=$SUBNET \
    --properties="spark.dataproc.lineage.enabled=true" \
    -- $PROJECT_ID $BUCKET_DIRECT_GCS

# Inspect Pipeline 1
python3 inspect_batch_lineage.py lineage-direct-gcs-28489

# Pipeline 2: BigQuery Connector via BigLake (External Tables)
gcloud dataproc batches submit spark \
    --project=$PROJECT_ID \
    --region=$REGION \
    --batch=lineage-biglake-connector-$RANDOM \
    --version=3.0 \
    --class=com.demo.lineage.Pipeline2BQExternal \
    --jars=gs://$BUCKET_MAIN/jars/lineage-poc-assembly-1.0.jar \
    --subnet=$SUBNET \
    --properties="spark.dataproc.lineage.enabled=true" \
    -- $PROJECT_ID $BUCKET_BQ_EXTERNAL

# Inspect Pipeline 2
python3 inspect_batch_lineage.py lineage-biglake-connector-11705

# Pipeline 3: Native Dataplex Path
gcloud dataproc batches submit spark \
    --project=$PROJECT_ID \
    --region=$REGION \
    --batch=lineage-native-dataplex-$RANDOM \
    --version=3.0 \
    --class=com.demo.lineage.Pipeline3NativeDataplex \
    --jars=gs://$BUCKET_MAIN/jars/lineage-poc-assembly-1.0.jar \
    --subnet=$SUBNET \
    --properties="spark.dataproc.lineage.enabled=true" \
    -- $PROJECT_ID $BUCKET_DISCOVERY

# Inspect Pipeline 3
python3 inspect_batch_lineage.py lineage-native-dataplex-12098

# Pipeline 4: Custom REST API Lineage
gcloud dataproc batches submit spark \
    --project=$PROJECT_ID \
    --region=$REGION \
    --batch=lineage-custom-api-$RANDOM \
    --version=3.0 \
    --class=com.demo.lineage.Pipeline4CustomLineage \
    --jars=gs://$BUCKET_MAIN/jars/lineage-poc-assembly-1.0.jar \
    -- $BUCKET_API

# Inspect Pipeline 4
python3 inspect_batch_lineage.py job_pipeline_4

# Pipeline 5 V2: Custom BigLake Rest Catalog Lineage Injection
CATALOG_NAME_V2="${PROJECT_ID}_demo_lineage_poc_blms_catalog_v2"

gcloud dataproc batches submit spark \
    --project=$PROJECT_ID \
    --region=$REGION \
    --batch="lineage-blms-custom-$RANDOM" \
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
```

### Inspecting Pipeline 5 V2 Lineage

Because Pipeline 5 overrides the default Dataproc batch UUID with a permanent `custom_job_id` to prevent history fragmentation, the inspection script must be queried using this explicit name:
```bash
export PROJECT_ID="<YOUR_PROJECT_ID_HERE>"
python3 inspect_batch_lineage.py pipeline5_v2
```

## Cleaning Up Persistent Lineage

Dataplex Data Lineage stores events natively within its API, meaning old processes will survive a standard `terraform destroy`.

If you are redeploying the POC and want a completely clean slate without "ghost" executions, execute the included cleanup script. It will forcefully iterate through the Dataplex REST API and recursively purge all lingering Process, Run, and Event definitions:

```bash
export PROJECT_ID="<YOUR_PROJECT_ID_HERE>"
python3 clean_lineage.py
```
