output "database_name" {
  description = "Glue database name."
  value       = aws_glue_catalog_database.this.name
}

output "crawler_name" {
  description = "Glue crawler name."
  value       = aws_glue_crawler.silver.name
}

output "crawler_role_arn" {
  description = "IAM role ARN assumed by the crawler."
  value       = aws_iam_role.crawler.arn
}
