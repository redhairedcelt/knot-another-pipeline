terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.50"
    }
  }
}

provider "aws" {
  region = var.region
}



data "archive_file" "plan_dates_zip" {
  type        = "zip"
  source_file = "${path.module}/lambda/plan_dates.py"
  output_path = "${path.module}/lambda/plan_dates.zip"
}

resource "aws_lambda_function" "plan_dates" {
  function_name = "${var.project_name}-plan-dates"
  role          = aws_iam_role.lambda_role.arn
  runtime       = "python3.11"
  handler       = "plan_dates.lambda_handler"
  filename      = data.archive_file.plan_dates_zip.output_path
  timeout       = 180
}

data "archive_file" "run_athena_zip" {
  type        = "zip"
  source_file = "${path.module}/lambda/run_athena.py"
  output_path = "${path.module}/lambda/run_athena.zip"
}

resource "aws_lambda_function" "run_athena" {
  function_name = "${var.project_name}-run-athena"
  role          = aws_iam_role.lambda_role.arn
  runtime       = "python3.11"
  handler       = "run_athena.lambda_handler"
  filename      = data.archive_file.run_athena_zip.output_path
  timeout       = 900
  environment {
    variables = {
      ATHENA_DATABASE   = var.athena_database
      ATHENA_WORKGROUP  = var.athena_workgroup
      ATHENA_RESULTS_S3 = var.athena_results_s3
      SILVER_TABLE      = var.silver_table
      UID_HOURLY_TABLE  = var.uid_hourly_table
      PAIRS_DAILY_TABLE = var.pairs_daily_table
      UID_HOURLY_S3     = var.uid_hourly_s3
      PAIRS_DAILY_S3    = var.pairs_daily_s3
    }
  }
}

resource "aws_iam_role" "lambda_role" {
  name = "${var.project_name}-lambda-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect    = "Allow",
      Principal = { Service = "lambda.amazonaws.com" },
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "lambda_policy" {
  role = aws_iam_role.lambda_role.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Resource = "*"
      },
      {
        Effect = "Allow",
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults"
        ],
        Resource = "*"
      },
      {
        Effect = "Allow",
        Action = [
          "glue:GetTable",
          "glue:GetDatabase",
          "glue:GetPartitions"
        ],
        Resource = "*"
      },
      {
        Effect = "Allow",
        Action = [
          "s3:ListBucket",
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ],
        Resource = [
          "${replace(var.athena_results_s3, "s3://", "arn:aws:s3:::")}*",
          "${replace(var.uid_hourly_s3, "s3://", "arn:aws:s3:::")}*",
          "${replace(var.pairs_daily_s3, "s3://", "arn:aws:s3:::")}*"
        ]
      }
    ]
  })
}

resource "aws_iam_role" "sfn_role" {
  name = "${var.project_name}-sfn-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect    = "Allow",
      Principal = { Service = "states.amazonaws.com" },
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "sfn_policy" {
  role = aws_iam_role.sfn_role.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = ["lambda:InvokeFunction"],
        Resource = [
          aws_lambda_function.plan_dates.arn,
          aws_lambda_function.run_athena.arn
        ]
      }
    ]
  })
}

locals {
  state_machine_definition = jsonencode({
    Comment = "Incremental uid_hourly_h3 + pairs_daily loader with DQ checks",
    StartAt = "PlanRun",
    States = {
      PlanRun = {
        Type     = "Task",
        Resource = aws_lambda_function.plan_dates.arn,
        Parameters = {
          "athena_database"   = var.athena_database,
          "silver_table"      = var.silver_table,
          "uid_hourly_table"  = var.uid_hourly_table,
          "pairs_daily_table" = var.pairs_daily_table,
          "athena_workgroup"  = var.athena_workgroup,
          "athena_results_s3" = var.athena_results_s3
        },
        ResultPath = "$.plan",
        Next       = "CheckCreateUID"
      },
      CheckCreateUID = {
        Type = "Choice",
        Choices = [
          {
            Variable      = "$.plan.create_uid_table",
            BooleanEquals = true,
            Next          = "CreateUIDTable"
          }
        ],
        Default = "CheckCreatePairs"
      },
      CreateUIDTable = {
        Type     = "Task",
        Resource = "arn:aws:states:::lambda:invoke",
        Parameters = {
          FunctionName = aws_lambda_function.run_athena.arn,
          Payload = {
            "action" = "create_uid_table"
          }
        },
        ResultPath = null,
        Next       = "CheckCreatePairs"
      },
      CheckCreatePairs = {
        Type = "Choice",
        Choices = [
          {
            Variable      = "$.plan.create_pairs_table",
            BooleanEquals = true,
            Next          = "CreatePairsTable"
          }
        ],
        Default = "CheckHasDates"
      },
      CreatePairsTable = {
        Type     = "Task",
        Resource = "arn:aws:states:::lambda:invoke",
        Parameters = {
          FunctionName = aws_lambda_function.run_athena.arn,
          Payload = {
            "action" = "create_pairs_table"
          }
        },
        ResultPath = null,
        Next       = "CheckHasDates"
      },
      CheckHasDates = {
        Type = "Choice",
        Choices = [
          {
            Variable           = "$.plan.date_count",
            NumericGreaterThan = 0,
            Next               = "ProcessDates"
          }
        ],
        Default = "NoWork"
      },
      NoWork = { Type = "Succeed" },
      ProcessDates = {
        Type           = "Map",
        ItemsPath      = "$.plan.dates",
        MaxConcurrency = 1,
        ItemSelector = {
          "dt.$" = "$$.Map.Item.Value.dt"
        },
        Iterator = {
          StartAt = "HourlyInsert",
          States = {
            HourlyInsert = {
              Type     = "Task",
              Resource = "arn:aws:states:::lambda:invoke",
              Parameters = {
                FunctionName = aws_lambda_function.run_athena.arn,
                Payload = {
                  "action" = "hourly_insert",
                  "dt.$"   = "$.dt"
                }
              },
              ResultPath = null,
              Next       = "HourlyDQ"
            },
            HourlyDQ = {
              Type     = "Task",
              Resource = "arn:aws:states:::lambda:invoke",
              Parameters = {
                FunctionName = aws_lambda_function.run_athena.arn,
                Payload = {
                  "action" = "hourly_dq",
                  "dt.$"   = "$.dt"
                }
              },
              ResultSelector = {
                "dup_count.$" = "$.Payload.dup_count"
              },
              ResultPath = "$.hourlyDQ",
              Next       = "CheckHourlyDQ"
            },
            CheckHourlyDQ = {
              Type = "Choice",
              Choices = [
                {
                  Variable      = "$.hourlyDQ.dup_count",
                  NumericEquals = 0,
                  Next          = "PairsInsert"
                }
              ],
              Default = "HourlyDQFailed"
            },
            HourlyDQFailed = {
              Type  = "Fail",
              Cause = "Hourly duplicate rows detected",
              Error = "DQ.HourlyDuplicates"
            },
            PairsInsert = {
              Type     = "Task",
              Resource = "arn:aws:states:::lambda:invoke",
              Parameters = {
                FunctionName = aws_lambda_function.run_athena.arn,
                Payload = {
                  "action" = "pairs_insert",
                  "dt.$"   = "$.dt"
                }
              },
              ResultPath = null,
              Next       = "PairsDQ"
            },
            PairsDQ = {
              Type     = "Task",
              Resource = "arn:aws:states:::lambda:invoke",
              Parameters = {
                FunctionName = aws_lambda_function.run_athena.arn,
                Payload = {
                  "action" = "pairs_dq",
                  "dt.$"   = "$.dt"
                }
              },
              ResultSelector = {
                "bad_hours.$"  = "$.Payload.bad_hours",
                "bad_scores.$" = "$.Payload.bad_scores"
              },
              ResultPath = "$.pairsDQ",
              Next       = "CheckPairsDQ"
            },
            CheckPairsDQ = {
              Type = "Choice",
              Choices = [
                {
                  Variable      = "$.pairsDQ.bad_hours",
                  NumericEquals = 0,
                  Next          = "CheckPairsDQScores"
                }
              ],
              Default = "PairsDQFailed"
            },
            CheckPairsDQScores = {
              Type = "Choice",
              Choices = [
                {
                  Variable      = "$.pairsDQ.bad_scores",
                  NumericEquals = 0,
                  Next          = "ProcessSuccess"
                }
              ],
              Default = "PairsDQFailed"
            },
            PairsDQFailed = {
              Type  = "Fail",
              Cause = "Pairs daily DQ violation",
              Error = "DQ.Pairs"
            },
            ProcessSuccess = { Type = "Succeed" }
          }
        },
        Next = "Done"
      },
      Done = { Type = "Succeed" }
    }
  })
}

resource "aws_sfn_state_machine" "uid_hourly_pipeline" {
  name       = "${var.project_name}-state-machine"
  role_arn   = aws_iam_role.sfn_role.arn
  definition = local.state_machine_definition
}
