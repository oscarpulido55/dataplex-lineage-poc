# Catalog Metadata Bucket specific for BigLake REST Service Pipeline 5 V2
resource "google_storage_bucket" "bucket_for_rest_catalog_v2" {
  name                        = "${var.project_id}-${var.bucket_blms_catalog_suffix_v2}"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
}

# The BigLake REST Catalog connection resource
resource "google_biglake_catalog" "rest_catalog_v2" {
  project  = var.project_id
  location = var.region
  name     = replace(google_storage_bucket.bucket_for_rest_catalog_v2.name, "-", "_")
}


