resource "aws_s3_bucket" "models" {
  bucket = "ml-pipeline-models-${var.env}-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket" "data" {
  bucket = "ml-pipeline-data-${var.env}-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket" "mlflow_artifacts" {
  bucket = "ml-pipeline-mlflow-${var.env}-${data.aws_caller_identity.current.account_id}"
}

# Shared settings for all buckets
locals {
  buckets = [
    aws_s3_bucket.models,
    aws_s3_bucket.data,
    aws_s3_bucket.mlflow_artifacts,
  ]
}

resource "aws_s3_bucket_versioning" "all" {
  for_each = { for b in local.buckets : b.bucket => b }
  bucket   = each.value.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "all" {
  for_each = { for b in local.buckets : b.bucket => b }
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "all" {
  for_each                = { for b in local.buckets : b.bucket => b }
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id
  rule {
    id     = "archive-old-data"
    status = "Enabled"
    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }
  }
}

data "aws_caller_identity" "current" {}

variable "env" { type = string }

output "model_bucket_name" { value = aws_s3_bucket.models.bucket }
output "model_bucket_arn" { value = aws_s3_bucket.models.arn }
output "data_bucket_name" { value = aws_s3_bucket.data.bucket }
output "mlflow_bucket_name" { value = aws_s3_bucket.mlflow_artifacts.bucket }
