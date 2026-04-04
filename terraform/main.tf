terraform {
  required_version = ">= 1.9.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.33"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.16"
    }
  }

  backend "s3" {
    # Configured via envs/<env>/backend.hcl
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "ml-pipeline"
      Environment = var.env
      ManagedBy   = "terraform"
    }
  }
}

provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_ca_certificate)
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name]
  }
}

provider "helm" {
  kubernetes {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_ca_certificate)
    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name]
    }
  }
}

# ============================================================
# Modules
# ============================================================
module "vpc" {
  source = "./modules/vpc"

  env         = var.env
  vpc_cidr    = var.vpc_cidr
  aws_region  = var.aws_region
}

module "eks" {
  source = "./modules/eks"

  env                = var.env
  vpc_id             = module.vpc.vpc_id
  private_subnet_ids = module.vpc.private_subnet_ids
  cluster_version    = var.eks_cluster_version

  gpu_instance_types = var.gpu_instance_types
  gpu_min_size       = var.gpu_min_size
  gpu_max_size       = var.gpu_max_size
}

module "msk" {
  source = "./modules/msk"

  env                = var.env
  vpc_id             = module.vpc.vpc_id
  private_subnet_ids = module.vpc.private_subnet_ids
  eks_security_group = module.eks.cluster_security_group_id
}

module "elasticache" {
  source = "./modules/elasticache"

  env                = var.env
  vpc_id             = module.vpc.vpc_id
  private_subnet_ids = module.vpc.private_subnet_ids
  eks_security_group = module.eks.cluster_security_group_id
}

module "aurora" {
  source = "./modules/aurora"

  env                = var.env
  vpc_id             = module.vpc.vpc_id
  private_subnet_ids = module.vpc.private_subnet_ids
  eks_security_group = module.eks.cluster_security_group_id
}

module "s3" {
  source = "./modules/s3"

  env = var.env
}

module "monitoring" {
  source = "./modules/monitoring"

  env          = var.env
  cluster_name = module.eks.cluster_name
}
