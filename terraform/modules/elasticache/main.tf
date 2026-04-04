resource "aws_security_group" "elasticache" {
  name_prefix = "ml-pipeline-cache-${var.env}-"
  vpc_id      = var.vpc_id

  ingress {
    description     = "Valkey from EKS"
    from_port       = 6379
    to_port         = 6379
    protocol        = "tcp"
    security_groups = [var.eks_security_group]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle { create_before_destroy = true }
}

resource "aws_elasticache_subnet_group" "this" {
  name       = "ml-pipeline-${var.env}"
  subnet_ids = var.private_subnet_ids
}

resource "aws_elasticache_replication_group" "this" {
  replication_group_id = "ml-pipeline-${var.env}"
  description          = "ML Pipeline Valkey cluster"
  engine               = "valkey"
  engine_version       = "8.0"
  node_type            = var.env == "prod" ? "cache.r7g.large" : "cache.t4g.small"
  num_cache_clusters   = var.env == "prod" ? 3 : 1

  subnet_group_name  = aws_elasticache_subnet_group.this.name
  security_group_ids = [aws_security_group.elasticache.id]

  at_rest_encryption_enabled = true
  transit_encryption_enabled = true
  auth_token                 = var.env == "prod" ? random_password.valkey[0].result : null

  automatic_failover_enabled = var.env == "prod"
  multi_az_enabled           = var.env == "prod"

  snapshot_retention_limit = var.env == "prod" ? 7 : 0
  snapshot_window          = "03:00-05:00"
  maintenance_window       = "sun:05:00-sun:07:00"

  apply_immediately = var.env != "prod"
}

resource "random_password" "valkey" {
  count   = var.env == "prod" ? 1 : 0
  length  = 32
  special = false
}

resource "aws_secretsmanager_secret" "valkey_password" {
  count = var.env == "prod" ? 1 : 0
  name  = "ml-pipeline/${var.env}/elasticache-password"
}

resource "aws_secretsmanager_secret_version" "valkey_password" {
  count         = var.env == "prod" ? 1 : 0
  secret_id     = aws_secretsmanager_secret.valkey_password[0].id
  secret_string = random_password.valkey[0].result
}

variable "env" { type = string }
variable "vpc_id" { type = string }
variable "private_subnet_ids" { type = list(string) }
variable "eks_security_group" { type = string }

output "primary_endpoint" {
  value = aws_elasticache_replication_group.this.primary_endpoint_address
}
