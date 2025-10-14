variable "aws_region" {
  description = "AWS region to deploy Glue resources."
  type        = string
  default     = "us-east-1"
}

variable "database_name" {
  description = "Name of the Glue catalog database."
  type        = string
  default     = "knap_ais"
}

variable "bucket_name" {
  description = "S3 bucket containing the AIS silver layer."
  type        = string
}

variable "silver_prefix" {
  description = "Root prefix for silver parquet data."
  type        = string
  default     = "silver/ais/"
}

variable "crawler_name" {
  description = "Name of the Glue crawler."
  type        = string
  default     = "knap-ais-silver"
}

variable "tags" {
  description = "Tags to apply to Glue resources."
  type        = map(string)
  default     = {}
}
