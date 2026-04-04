resource "aws_security_group" "msk" {
  name_prefix = "ml-pipeline-msk-${var.env}-"
  vpc_id      = var.vpc_id

  ingress {
    description     = "Kafka from EKS"
    from_port       = 9092
    to_port         = 9098
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

resource "aws_msk_cluster" "this" {
  cluster_name           = "ml-pipeline-${var.env}"
  kafka_version          = "3.6.0"
  number_of_broker_nodes = length(var.private_subnet_ids)

  broker_node_group_info {
    instance_type  = var.env == "prod" ? "kafka.m5.large" : "kafka.t3.small"
    client_subnets = var.private_subnet_ids
    storage_info {
      ebs_storage_info {
        volume_size = var.env == "prod" ? 100 : 20
      }
    }
    security_groups = [aws_security_group.msk.id]
  }

  client_authentication {
    sasl {
      iam = true
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = "/aws/msk/ml-pipeline-${var.env}"
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.this.arn
    revision = aws_msk_configuration.this.latest_revision
  }
}

resource "aws_msk_configuration" "this" {
  name              = "ml-pipeline-${var.env}"
  kafka_versions    = ["3.6.0"]
  server_properties = <<-PROPERTIES
    auto.create.topics.enable=true
    default.replication.factor=3
    min.insync.replicas=2
    num.partitions=3
    log.retention.hours=168
  PROPERTIES
}

resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/ml-pipeline-${var.env}"
  retention_in_days = 14
}

variable "env" { type = string }
variable "vpc_id" { type = string }
variable "private_subnet_ids" { type = list(string) }
variable "eks_security_group" { type = string }

output "bootstrap_brokers_iam" {
  value = aws_msk_cluster.this.bootstrap_brokers_sasl_iam
}

output "cluster_arn" {
  value = aws_msk_cluster.this.arn
}
