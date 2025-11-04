# Terraform Environment Setup

This guide walks through provisioning the AIS data lake infrastructure with Terraform. It covers the S3 bucket used for bronze/silver/gold data and the Glue catalog resources that make the silver layer queryable in Athena.

## Prerequisites
- Terraform `>= 1.6.0` (see `infra/terraform/*/versions.tf`).
- AWS credentials with permissions to create S3, Glue, IAM, and CloudWatch Logs resources. Export standard environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`) or set `AWS_PROFILE`.
- Clone this repository locally and open a terminal at the repo root.

## 1. Provision the AIS Data Bucket
The `infra/terraform/ais_bucket` module creates the shared S3 bucket (`bronze/`, `silver/`, `gold/`). Pick a globally unique bucket name before running the commands.

```bash
cd infra/terraform/ais_bucket

# Optional: capture variables once instead of passing -var flags each time.
cat <<'EOF' > terraform.tfvars
bucket_name = "knap-ais"
aws_region  = "us-east-1"
tags = {
  Environment = "dev"
  Owner       = "data-platform"
}
EOF

terraform init
terraform plan      # review the proposed infrastructure
terraform apply     # deploy the bucket; confirm when prompted
```

Key outputs:
- `bucket_id`: canonical bucket name.
- `bucket_arn`: ARN for IAM policy references.

> To tear the bucket down later (this empties the bucket first), run `terraform destroy`.

## 2. Register the Silver Layer in the Glue Catalog
After the bucket exists, create the Glue database, IAM role, and crawler that surfaces the silver partitions to Athena.

```bash
cd ../ais_glue_catalog   # relative to the previous directory

cat <<'EOF' > terraform.tfvars
bucket_name   = "knap-ais"
aws_region    = "us-east-1"
database_name = "knap_ais"
crawler_name  = "knap-ais-silver"
silver_prefix = "silver/ais/"
tags = {
  Environment = "dev"
  Owner       = "data-platform"
}
EOF

terraform init
terraform plan
terraform apply
```

After `apply` completes:
1. Open the AWS Glue console and manually start the new crawler (or configure a schedule) to register the `silver_ais` table.
2. Query the silver data from Athena under the `knap_ais` database.

Outputs to note:
- `database_name`: Glue database that Athena reads.
- `crawler_name`: Useful for scheduling or automation.
- `crawler_role_arn`: IAM role assumed by the crawler; attach additional permissions here if you expand coverage.

## 3. Next Steps
- Load NOAA files into the bronze layer and transform them with `pipelines/ais_pipeline.py`.
- Refresh the gold tables via `pipelines/refresh_gold_tables.py --gold-root s3://knap-ais/gold`.
- Add Step Functions or other orchestration once the manual workflow is validated.

Keep Terraform state files (`terraform.tfstate`) under source control exclusion (already handled via `.gitignore`); use remote state (e.g., S3 backend) before multiple teammates apply infrastructure changes.
