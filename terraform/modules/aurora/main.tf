resource "aws_security_group" "aurora" {
  name_prefix = "ml-pipeline-aurora-${var.env}-"
  vpc_id      = var.vpc_id

  ingress {
    description     = "PostgreSQL from EKS"
    from_port       = 5432
    to_port         = 5432
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

resource "aws_db_subnet_group" "this" {
  name       = "ml-pipeline-${var.env}"
  subnet_ids = var.private_subnet_ids
}

resource "aws_rds_cluster" "this" {
  cluster_identifier = "ml-pipeline-${var.env}"
  engine             = "aurora-postgresql"
  engine_version     = "16.4"
  engine_mode        = "provisioned"

  database_name   = "mlflow"
  master_username = "mlflow_admin"
  master_password = random_password.aurora.result

  db_subnet_group_name   = aws_db_subnet_group.this.name
  vpc_security_group_ids = [aws_security_group.aurora.id]

  storage_encrypted = true
  deletion_protection = var.env == "prod"
  skip_final_snapshot = var.env != "prod"

  backup_retention_period = var.env == "prod" ? 14 : 1
  preferred_backup_window = "02:00-03:00"

  serverlessv2_scaling_configuration {
    min_capacity = var.env == "prod" ? 1.0 : 0.5
    max_capacity = var.env == "prod" ? 8.0 : 2.0
  }
}

resource "aws_rds_cluster_instance" "this" {
  count              = var.env == "prod" ? 2 : 1
  identifier         = "ml-pipeline-${var.env}-${count.index}"
  cluster_identifier = aws_rds_cluster.this.id
  instance_class     = "db.serverless"
  engine             = aws_rds_cluster.this.engine
  engine_version     = aws_rds_cluster.this.engine_version
}

resource "random_password" "aurora" {
  length  = 32
  special = false
}

resource "aws_secretsmanager_secret" "aurora_password" {
  name = "ml-pipeline/${var.env}/aurora-password"
}

resource "aws_secretsmanager_secret_version" "aurora_password" {
  secret_id = aws_secretsmanager_secret.aurora_password.id
  secret_string = jsonencode({
    username = aws_rds_cluster.this.master_username
    password = random_password.aurora.result
    host     = aws_rds_cluster.this.endpoint
    port     = 5432
    dbname   = aws_rds_cluster.this.database_name
  })
}

variable "env" { type = string }
variable "vpc_id" { type = string }
variable "private_subnet_ids" { type = list(string) }
variable "eks_security_group" { type = string }

output "cluster_endpoint" { value = aws_rds_cluster.this.endpoint }
output "cluster_reader_endpoint" { value = aws_rds_cluster.this.reader_endpoint }
