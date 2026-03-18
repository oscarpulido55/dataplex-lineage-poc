resource "google_dataplex_lake" "lineage_poc_lake" {
  name        = "demo-lineage-auto-discovery-lake"
  project     = var.project_id
  location    = var.region
  description = "Dataplex Lake for Lineage POC"
}

resource "google_dataplex_zone" "lineage_poc_zone" {
  name     = "demo-lineage-poc-auto-discovery-zone"
  lake     = google_dataplex_lake.lineage_poc_lake.name
  project  = var.project_id
  location = var.region
  type     = "RAW"

  discovery_spec {
    enabled = true
  }

  resource_spec {
    location_type = "SINGLE_REGION"
  }
}

resource "google_dataplex_asset" "custom_catalog_asset" {
  name          = "custom-catalog-asset"
  lake          = google_dataplex_lake.lineage_poc_lake.name
  dataplex_zone = google_dataplex_zone.lineage_poc_zone.name
  project       = var.project_id
  location      = var.region

  discovery_spec {
    enabled = true
  }

  resource_spec {
    name = "projects/${var.project_id}/buckets/${google_storage_bucket.auto_discovery.name}"
    type = "STORAGE_BUCKET"
  }

  depends_on = [
    google_storage_bucket_object.auto_discovery_source,
    google_storage_bucket_object.auto_discovery_dest_dummy
  ]
}
