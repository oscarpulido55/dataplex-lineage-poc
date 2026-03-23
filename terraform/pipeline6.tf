# Pipeline 6: Isolating Uniqueness Experiments

resource "google_bigquery_dataset" "pipeline6_dataset" {
  dataset_id    = "demo_lineage_poc_pipeline6_native"
  friendly_name = "Lineage POC BQ Pure Native Pipeline 6"
  description   = "Dataset for Pipeline 6 Uniqueness Experiments"
  location      = var.region
}

resource "google_bigquery_table" "pipeline6_source" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.pipeline6_dataset.dataset_id
  table_id            = "pipeline6_source"
  deletion_protection = false

  # A strict schema is not defined here because the load job with autodetect will infer it
  # from the parquet file.
}

resource "google_bigquery_job" "load_pipeline6_source" {
  job_id   = "load_pipeline6_source_data_${replace(timestamp(), ":", "_")}"
  project  = var.project_id
  location = var.region

  load {
    # Reuse the dummy data already uploaded to the bq_external bucket
    source_uris = ["gs://${google_storage_bucket.bq_external.name}/source/data.parquet"]

    destination_table {
      project_id = var.project_id
      dataset_id = google_bigquery_dataset.pipeline6_dataset.dataset_id
      table_id   = google_bigquery_table.pipeline6_source.table_id
    }

    source_format     = "PARQUET"
    autodetect        = true
    create_disposition = "CREATE_IF_NEEDED"
    write_disposition  = "WRITE_TRUNCATE"
  }
}
