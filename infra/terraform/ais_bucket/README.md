# AIS Bucket Module

This Terraform module provisions a secure S3 bucket intended to back the AIS bronze, silver, and gold layers produced by `pipelines/ais_pipeline.py`.

## Features

- Bucket-level public access block
- Server-side encryption with AWS-managed keys (SSE-S3)
- Versioning enabled to protect against accidental overwrites
- Lifecycle policy to clean up incomplete multipart uploads
- Consistent tagging to support cost allocation

## Usage

```hcl
module "ais_bucket" {
  source      = "./infra/terraform/ais_bucket"
  bucket_name = "knap-ais"
  aws_region  = "us-east-1"
  tags = {
    Environment = "dev"
    Owner       = "data-platform"
  }
}
```

Initialise and apply:

```bash
cd infra/terraform/ais_bucket
terraform init
terraform apply -var="bucket_name=knap-ais" -var="aws_region=us-east-1"
```

Extend the module by attaching IAM roles and policies that allow your orchestration runtime (ECS task, Lambda function, or Step Functions activities) to read `bronze/*` and read/write `silver/*` and `gold/*`.
