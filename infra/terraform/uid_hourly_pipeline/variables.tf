variable "project_name" {
  type = string
}

variable "region" {
  type = string
}

variable "athena_database" {
  type = string
}

variable "athena_workgroup" {
  type = string
}

variable "athena_results_s3" {
  type = string
}

variable "silver_table" {
  type = string
}

variable "uid_hourly_table" {
  type = string
}

variable "pairs_daily_table" {
  type = string
}

variable "uid_hourly_s3" {
  type = string
}

variable "pairs_daily_s3" {
  type = string
}
