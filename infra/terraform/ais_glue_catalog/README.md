# AIS Glue Catalog

Terraform module that provisions a Glue catalog database and crawler to register the AIS silver layer in Athena.

## Resources

- Glue catalog database (`knap_ais` by default)
- IAM role + inline policy granting the crawler read-only access to the silver S3 prefix
- Glue crawler targeting `s3://<bucket>/silver/ais/`

## Usage

```hcl
module "ais_glue_catalog" {
  source       = "./infra/terraform/ais_glue_catalog"
  aws_region   = "us-east-1"
  bucket_name  = "knap-ais-bronze-silver"
  database_name = "knap_ais"
  crawler_name  = "knap-ais-silver"
}
```

Then initialise and apply:

```bash
cd infra/terraform/ais_glue_catalog
terraform init
terraform apply -var="bucket_name=knap-ais-bronze-silver"
```

After provisioning, start the crawler manually (or add a schedule in the AWS console) so the `silver_ais` table appears in the `knap_ais` Glue database and becomes queryable from Athena.
