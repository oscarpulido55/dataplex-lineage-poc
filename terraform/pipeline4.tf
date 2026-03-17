resource "google_dataplex_entry_group" "pipeline4_entry_group" {
  project        = var.project_id
  location       = var.region
  entry_group_id = "demo-lineage-poc-lineage-api-group"

  description  = "Entry Group anchoring the Custom Pipeline 4 OpenLineage graphs"
  display_name = "Pipeline 4 Custom Native Lineage"
}

resource "google_dataplex_entry_type" "pipeline4_entry_type" {
  project       = var.project_id
  location      = var.region
  entry_type_id = "demo-lineage-poc-custom-type"

  description  = "Custom Entry Type to allow Spark job to manually mint assets"
  display_name = "Custom FileSet"
  
  type_aliases = ["FILESET"]
  platform     = "Cloud Storage"
  system       = "GCS Pipeline"
}

resource "google_dataplex_aspect_type" "pipeline4_custom_aspect" {
  project        = var.project_id
  location       = var.region
  aspect_type_id = "demo-lineage-poc-custom-aspect"

  description  = "Custom Aspect for Lineage POC storing file format and framework metadata"
  display_name = "Demo Lineage POC Custom Aspect"

  metadata_template   = <<EOF
{
  "name": "customAspectRatio",
  "type": "record",
  "recordFields": [
    {
      "name": "format",
      "type": "string",
      "index": 1
    },
    {
      "name": "framework",
      "type": "string",
      "index": 2
    },
    {
      "name": "path",
      "type": "string",
      "index": 3
    }
  ]
}
EOF
}
