# knot-another-pipeline
Experiments identifying patterns in AIS activity using reproducible IaC and data pipelines.

# Summary and Project Goals

This is a project to experiment and demonstrate best practices with building reusable, scalabe data pipelines, infrastructure as code, applications to support analysis, and analytic to surface new insights from complex data.

## Data

This project will leverage data from the publically available [NOAA Marine Cadastre Vessel Traffic Data](https://hub.marinecadastre.gov/pages/vesseltraffic) site.

## Apps
### Geo-Temporal Data Explorer
Built using Streamlist and Pydeck, this application will allow a user to load a csv file, visualize the track on a map, adjust the time slider, and select the specific track ids or uids desired. 

## Getting Started

- Spin up the shared conda environment: `conda env update --file apps/environment.yml --prune`.
- Review the AIS ingestion docs at `docs/ais_pipeline.md` for end-to-end guidance on downloading NOAA AIS data, landing it in a bronze S3 layer, and curating a silver Parquet dataset.
- Provision infrastructure with the Terraform module in `infra/terraform/ais_bucket` when you are ready to run the pipeline in AWS.
