resource "aws_prometheus_workspace" "this" {
  alias = "ml-pipeline-${var.env}"

  logging_configuration {
    log_group_arn = "${aws_cloudwatch_log_group.amp.arn}:*"
  }
}

resource "aws_cloudwatch_log_group" "amp" {
  name              = "/aws/amp/ml-pipeline-${var.env}"
  retention_in_days = 14
}

resource "aws_grafana_workspace" "this" {
  name                     = "ml-pipeline-${var.env}"
  account_access_type      = "CURRENT_ACCOUNT"
  authentication_providers = ["AWS_SSO"]
  permission_type          = "SERVICE_MANAGED"
  role_arn                 = aws_iam_role.grafana.arn

  data_sources = ["PROMETHEUS", "CLOUDWATCH"]
}

resource "aws_iam_role" "grafana" {
  name = "ml-pipeline-grafana-${var.env}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = "grafana.amazonaws.com" }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "grafana_amp" {
  name = "amp-access"
  role = aws_iam_role.grafana.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = [
        "aps:ListWorkspaces",
        "aps:DescribeWorkspace",
        "aps:QueryMetrics",
        "aps:GetLabels",
        "aps:GetSeries",
        "aps:GetMetricMetadata",
      ]
      Resource = "*"
    }]
  })
}

variable "env" { type = string }
variable "cluster_name" { type = string }

output "amp_workspace_id" { value = aws_prometheus_workspace.this.id }
output "amp_remote_write_url" {
  value = "${aws_prometheus_workspace.this.prometheus_endpoint}api/v1/remote_write"
}
output "grafana_endpoint" { value = aws_grafana_workspace.this.endpoint }
