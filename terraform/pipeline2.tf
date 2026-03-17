# Pipeline 2: BQ Connector via External Tables
# Requires BigQuery Storage API Read integration over a BigLake connection.

resource "google_bigquery_dataset" "pipeline2_dataset" {
  dataset_id    = "wpp_lineage_poc_bq_native"
  friendly_name = "Lineage POC BQ Native Connector"
  description   = "Dataset for Pipeline 2 (BigQuery Storage API Reads over BigLake)"
  location      = var.region
}

resource "google_bigquery_connection" "bq_native_biglake_conn" {
  connection_id = "wpp-lineage-poc-biglake-conn"
  project       = var.project_id
  location      = var.region
  friendly_name = "BigLake Connection for Native Lineage"

  cloud_resource {}
}

resource "google_project_iam_member" "biglake_sa_storage_viewer" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_bigquery_connection.bq_native_biglake_conn.cloud_resource[0].service_account_id}"
}

resource "time_sleep" "wait_for_iam" {
  depends_on = [google_project_iam_member.biglake_sa_storage_viewer]
  create_duration = "30s"
}

resource "google_bigquery_table" "native_bq_tables_source" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.pipeline2_dataset.dataset_id
  table_id            = "native_bq_tables_source"
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${google_storage_bucket.bq_native.name}/source/*.parquet"]
    connection_id = google_bigquery_connection.bq_native_biglake_conn.name
  }
  depends_on = [google_storage_bucket_object.bq_native_source, time_sleep.wait_for_iam]
}

resource "google_bigquery_table" "native_bq_tables_dest" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.pipeline2_dataset.dataset_id
  table_id            = "native_bq_tables_dest"
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${google_storage_bucket.bq_native.name}/dest/*.parquet"]
    connection_id = google_bigquery_connection.bq_native_biglake_conn.name
  }
  depends_on = [google_storage_bucket_object.bq_native_dest_dummy, time_sleep.wait_for_iam]
}
