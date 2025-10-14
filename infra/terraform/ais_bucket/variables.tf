variable "bucket_name" {
  description = "Globally-unique name for the AIS bronze/silver bucket."
  type        = string
}

variable "aws_region" {
  description = "AWS region used for the S3 bucket."
  type        = string
  default     = "us-east-1"
}

variable "tags" {
  description = "Common tags to apply to created resources."
  type        = map(string)
  default     = {}
}
