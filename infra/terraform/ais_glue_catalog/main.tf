provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}

locals {
  common_tags = merge(
    {
      Application = "knot-another-pipeline"
      Layer       = "ais-lakehouse"
      ManagedBy   = "terraform"
    },
    var.tags
  )

  silver_uri  = "s3://${var.bucket_name}/${trim(var.silver_prefix, "/")}/"
  catalog_arn = "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog"
}

resource "aws_glue_catalog_database" "this" {
  name = var.database_name
  tags = local.common_tags
}

resource "aws_iam_role" "crawler" {
  name               = "${var.crawler_name}-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "crawler_policy" {
  statement {
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:ListBucket",
      "s3:ListBucketVersions"
    ]
    resources = [
      "arn:aws:s3:::${var.bucket_name}",
      "arn:aws:s3:::${var.bucket_name}/${trim(var.silver_prefix, "/")}*"
    ]
  }

  statement {
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetTableVersion",
      "glue:GetTableVersions",
      "glue:DeleteTable",
      "glue:BatchCreatePartition",
      "glue:CreatePartition",
      "glue:UpdatePartition",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchDeletePartition",
      "glue:BatchGetPartition"
    ]
    resources = [
      local.catalog_arn,
      aws_glue_catalog_database.this.arn,
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.this.name}/*",
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:partition/${aws_glue_catalog_database.this.name}/*"
    ]
  }

  statement {
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "crawler" {
  name   = "${var.crawler_name}-policy"
  role   = aws_iam_role.crawler.id
  policy = data.aws_iam_policy_document.crawler_policy.json
}

resource "aws_glue_crawler" "silver" {
  name          = var.crawler_name
  role          = aws_iam_role.crawler.arn
  database_name = aws_glue_catalog_database.this.name
  description   = "Crawler for AIS silver parquet dataset."
  classifiers   = []
  table_prefix  = "silver_"
  tags          = local.common_tags

  s3_target {
    path        = local.silver_uri
    sample_size = 1
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })
}
