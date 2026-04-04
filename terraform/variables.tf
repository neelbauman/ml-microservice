variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "ap-northeast-1"
}

variable "env" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "vpc_cidr" {
  description = "VPC CIDR block"
  type        = string
  default     = "10.0.0.0/16"
}

variable "eks_cluster_version" {
  description = "EKS Kubernetes version"
  type        = string
  default     = "1.30"
}

variable "gpu_instance_types" {
  description = "GPU node instance types"
  type        = list(string)
  default     = ["g5.xlarge"]
}

variable "gpu_min_size" {
  description = "GPU node group minimum size"
  type        = number
  default     = 0
}

variable "gpu_max_size" {
  description = "GPU node group maximum size"
  type        = number
  default     = 4
}
