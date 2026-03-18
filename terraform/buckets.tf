# ==========================================
# Core Infrastructure: GCS Buckets
# ==========================================

locals {
  bucket_main           = "${var.project_id}-${var.bucket_main_suffix}"
  bucket_direct_gcs    = "${var.project_id}-${var.bucket_direct_gcs_suffix}"
  bucket_auto_discovery = "${var.project_id}-${var.bucket_auto_discovery_suffix}"
  bucket_lineage_api    = "${var.project_id}-${var.bucket_lineage_api_suffix}"
  bucket_bq_external    = "${var.project_id}-${var.bucket_bq_external_suffix}"
  bucket_blms_data_v2   = "${var.project_id}-${var.bucket_blms_data_suffix_v2}"
}

# Main bucket for JARs, POC scripts, and temp storage
resource "google_storage_bucket" "main" {
  name                        = local.bucket_main
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
}

# Isolated Bucket for Pipeline 1: Direct GCS
resource "google_storage_bucket" "direct_gcs" {
  name                        = local.bucket_direct_gcs
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
}

# Isolated Bucket for Pipeline 3: Auto Discovery
resource "google_storage_bucket" "auto_discovery" {
  name                        = local.bucket_auto_discovery
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
}

# Isolated Bucket for Pipeline 4: Lineage API
resource "google_storage_bucket" "lineage_api" {
  name                        = local.bucket_lineage_api
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
}

# Isolated Bucket for Pipeline 2: BQ External
resource "google_storage_bucket" "bq_external" {
  name                        = local.bucket_bq_external
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
}

# Isolated Bucket for Pipeline 5 V2: BLMS Data
resource "google_storage_bucket" "blms_data_v2" {
  name                        = local.bucket_blms_data_v2
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
}

# ==========================================
# Automated Sample Data Uploads
# ==========================================

resource "google_storage_bucket_object" "direct_gcs_source" {
  name   = "source/data.parquet"
  source = "../data_gen/external_bq_tables_source.parquet" # Note: reusing generic test file
  bucket = google_storage_bucket.direct_gcs.name
}

resource "google_storage_bucket_object" "direct_gcs_dest_dummy" {
  name   = "dest/dummy.parquet"
  source = "../data_gen/dummy_dest.parquet"
  bucket = google_storage_bucket.direct_gcs.name
}

resource "google_storage_bucket_object" "auto_discovery_source" {
  name   = "source/data.parquet"
  source = "../data_gen/custom_catalog_entries_source.parquet"
  bucket = google_storage_bucket.auto_discovery.name
}

resource "google_storage_bucket_object" "auto_discovery_dest_dummy" {
  name   = "dest/dummy.parquet"
  source = "../data_gen/dummy_dest.parquet"
  bucket = google_storage_bucket.auto_discovery.name
}

resource "google_storage_bucket_object" "lineage_api_source" {
  name   = "source/data.parquet"
  source = "../data_gen/api_managed_lineage_source.parquet"
  bucket = google_storage_bucket.lineage_api.name
}

resource "google_storage_bucket_object" "bq_external_source" {
  name   = "source/data.parquet"
  source = "../data_gen/native_bq_source.parquet"
  bucket = google_storage_bucket.bq_external.name
}

resource "google_storage_bucket_object" "bq_external_dest_dummy" {
  name   = "dest/dummy.parquet"
  source = "../data_gen/dummy_dest.parquet"
  bucket = google_storage_bucket.bq_external.name
}

# Upload data to the BLMS Data bucket
resource "google_storage_bucket_object" "blms_data_source" {
  name   = "source/data.parquet"
  source = "../data_gen/native_bq_source.parquet" # Reuse the same native BQ source dummy data
  bucket = google_storage_bucket.blms_data_v2.name
}

# Upload dummy dest data to the BLMS Data bucket
resource "google_storage_bucket_object" "blms_data_dest_dummy" {
  name   = "dest/dummy.parquet"
  source = "../data_gen/dummy_dest.parquet"       # This is used just to create the target folder structure
  bucket = google_storage_bucket.blms_data_v2.name
}
