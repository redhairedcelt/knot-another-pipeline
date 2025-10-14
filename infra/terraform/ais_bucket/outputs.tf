output "bucket_id" {
  description = "Canonical ID of the AIS data bucket."
  value       = aws_s3_bucket.this.id
}

output "bucket_arn" {
  description = "ARN for the AIS data bucket."
  value       = aws_s3_bucket.this.arn
}
