variable "project_id" {
  description = "The GCP Project ID where resources will be deployed."
  type        = string
  # No default, force the user or CI to provide it via -var="project_id=..." or terraform.tfvars
}

variable "region" {
  description = "The GCP region for deployment."
  type        = string
  default     = "us-east1"
}

# The suffixes of the buckets to create (these will be prefixed with project_id in buckets.tf)
variable "bucket_main_suffix" {
  type    = string
  default = "wpp_lineage_poc"
}

variable "bucket_bq_external_suffix" {
  type    = string
  default = "wpp_lineage_poc_bq_external"
}

variable "bucket_auto_discovery_suffix" {
  type    = string
  default = "wpp_lineage_poc_auto_discovery"
}

variable "bucket_lineage_api_suffix" {
  type    = string
  default = "wpp_lineage_poc_lineage_api"
}

variable "bucket_bq_native_suffix" {
  type    = string
  default = "wpp_lineage_poc_bq_native"
}
