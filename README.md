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
export BUCKET_BQ="${PROJECT_ID}-demo_lineage_poc_bq_external"
export BUCKET_DISCOVERY="${PROJECT_ID}-demo_lineage_poc_auto_discovery"
export BUCKET_API="${PROJECT_ID}-demo_lineage_poc_lineage_api"
export BUCKET_NAT="${PROJECT_ID}-demo_lineage_poc_bq_native"
```

## Compiling the Spark Job
Because Dataproc natively bundles `protobuf`, `grpc`, and the Spark BigQuery connector, we mark these dependencies as `% "provided"` to prevent JVM "Classpath Hell".
```bash
sbt clean assembly
gcloud storage cp target/scala-2.12/lineage-poc-assembly-1.0.jar gs://$BUCKET_MAIN/jars/lineage-poc-assembly-1.0.jar --project=$PROJECT_ID
```

## Running the Pipelines


All pipelines utilize Dataproc Serverless to stitch Spark operations into the Dataplex Universal Catalog.

### Key Architectural Findings & Column-Level Lineage

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
  * **Note on Pipeline 2 vs 3 Similarity**: Notice that if we had written Pipeline 3 using the `spark-bigquery` connector (exactly like Pipeline 2), the lineage *would* render in the UI. The only difference between those two scenarios is how the table definition was created (Terraform BigLake manual creation vs. Dataplex Auto-Discovery "Upgrade to Managed"). If you use the connector, OpenLineage emits the `bigquery:` FQN, which renders nicely. We deliberately wrote Pipeline 3 exactly like Pipeline 1 (using direct `gcs:` reads) to show that Pipeline 1-style purely-storage lineage fails to map to Native Dataplex entities out of the box.

#### 4. Pipeline 4: Custom REST API
* **Prerequisites**: `google_dataplex_entry_group`, `google_dataplex_entry_type`, `google_dataplex_aspect_type`
  * **Why is an Entry Group required?**: Because we are bypassing the Auto-Discovery `Lake/Zone` hierarchy, we must explicitly provision a purely logical `Entry Group` to serve as the structural anchor for our manually minted `custom` Lineage Entries and their `Entry/Aspect Types`.
* **Column-Level Lineage**: **Not Supported**. The `custom:` FQN format currently maps to standard entity definitions. While custom aspects can hold arbitrary schema text, native Lineage UI visualization for columns relies on Google-managed BigQuery/Dataplex Discovery entities.
* **Pros**: Absolute control. Enforces 1:1 lineage linking via explicit FQNs (`custom:namespace.target`). Impossible to produce "ghost nodes". Supports custom metadata "Aspects" natively.
* **Cons**: High engineering cost. Requires managing custom API integration within the Spark application code. No native column-level graph.

### Execution Commands

```bash
# Pipeline 1: BQ External Tables (Direct GCS Read)
gcloud dataproc batches submit spark \
    --project=$PROJECT_ID \
    --region=$REGION \
    --batch=lineage-direct-gcs \
    --class=com.demo.lineage.Pipeline1DirectGcsRead \
    --jars=gs://$BUCKET_MAIN/jars/lineage-poc-assembly-1.0.jar \
    --subnet=$SUBNET \
    --properties="spark.dataproc.lineage.enabled=true" \
    -- $PROJECT_ID $BUCKET_BQ

# Pipeline 2: BigQuery Connector via BigLake
gcloud dataproc batches submit spark \
    --project=$PROJECT_ID \
    --region=$REGION \
    --batch=lineage-biglake-connector \
    --class=com.demo.lineage.Pipeline2BQNativePath \
    --jars=gs://$BUCKET_MAIN/jars/lineage-poc-assembly-1.0.jar \
    --subnet=$SUBNET \
    --properties="spark.dataproc.lineage.enabled=true" \
    -- $PROJECT_ID $BUCKET_NAT

# Pipeline 3: Native Dataplex Path
gcloud dataproc batches submit spark \
    --project=$PROJECT_ID \
    --region=$REGION \
    --batch=lineage-native-dataplex \
    --class=com.demo.lineage.Pipeline3NativeDataplex \
    --jars=gs://$BUCKET_MAIN/jars/lineage-poc-assembly-1.0.jar \
    --subnet=$SUBNET \
    --properties="spark.dataproc.lineage.enabled=true" \
    -- $PROJECT_ID $BUCKET_DISCOVERY

# Pipeline 4: Custom REST API Lineage
gcloud dataproc batches submit spark \
    --project=$PROJECT_ID \
    --region=$REGION \
    --batch=lineage-custom-api \
    --class=com.demo.lineage.Pipeline4CustomLineage \
    --jars=gs://$BUCKET_MAIN/jars/lineage-poc-assembly-1.0.jar \
    --subnet=$SUBNET \
    -- $BUCKET_API
```

## Cleaning Up Persistent Lineage

Dataplex Data Lineage stores events natively within its API, meaning old processes will survive a standard `terraform destroy`.

If you are redeploying the POC and want a completely clean slate without "ghost" executions, execute the included cleanup script. It will forcefully iterate through the Dataplex REST API and recursively purge all lingering Process, Run, and Event definitions:

```bash
export PROJECT_ID="<YOUR_PROJECT_ID_HERE>"
python3 clean_lineage.py
```
