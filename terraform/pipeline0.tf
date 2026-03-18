# Pipeline 0: Pure BQ Native (No GCS interaction in Spark)

resource "google_bigquery_dataset" "pipeline0_dataset" {
  dataset_id    = "demo_lineage_poc_pipeline0_native"
  friendly_name = "Lineage POC BQ Pure Native"
  description   = "Dataset for Pipeline 0 (BigQuery Native Reads and Writes)"
  location      = var.region
}

resource "google_bigquery_table" "pipeline0_source" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.pipeline0_dataset.dataset_id
  table_id            = "pure_native_source"
  deletion_protection = false

  # We don't define a strict schema here because the load job with autodetect will infer it
  # from the parquet file.
}

resource "google_bigquery_job" "load_pipeline0_source" {
  job_id   = "load_pipeline0_source_data_${replace(timestamp(), ":", "_")}"
  project  = var.project_id
  location = var.region

  load {
    # Reuse the dummy data already uploaded to the bq_external bucket
    source_uris = ["gs://${google_storage_bucket.bq_external.name}/source/data.parquet"]

    destination_table {
      project_id = var.project_id
      dataset_id = google_bigquery_dataset.pipeline0_dataset.dataset_id
      table_id   = google_bigquery_table.pipeline0_source.table_id
    }

    source_format     = "PARQUET"
    autodetect        = true
    create_disposition = "CREATE_IF_NEEDED"
    write_disposition  = "WRITE_TRUNCATE"
  }
}
