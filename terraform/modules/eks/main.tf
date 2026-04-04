module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.24"

  cluster_name    = "ml-pipeline-${var.env}"
  cluster_version = var.cluster_version

  vpc_id     = var.vpc_id
  subnet_ids = var.private_subnet_ids

  cluster_endpoint_public_access  = true
  cluster_endpoint_private_access = true

  enable_cluster_creator_admin_permissions = true

  cluster_addons = {
    coredns                = { most_recent = true }
    kube-proxy             = { most_recent = true }
    vpc-cni                = { most_recent = true }
    aws-ebs-csi-driver     = { most_recent = true }
  }

  eks_managed_node_groups = {
    system = {
      instance_types = ["m6i.large"]
      min_size       = 2
      max_size       = 4
      desired_size   = 2

      labels = { role = "system" }

      iam_role_additional_policies = {
        AmazonSSMManagedInstanceCore = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
      }
    }

    app = {
      instance_types = ["c5.xlarge"]
      min_size       = 2
      max_size       = 10
      desired_size   = 3

      labels = { role = "app" }
    }

    gpu = {
      instance_types = var.gpu_instance_types
      ami_type       = "AL2_x86_64_GPU"
      min_size       = var.gpu_min_size
      max_size       = var.gpu_max_size
      desired_size   = max(var.gpu_min_size, 1)

      labels = { role = "gpu" }
      taints = {
        gpu = {
          key    = "nvidia.com/gpu"
          value  = "true"
          effect = "NO_SCHEDULE"
        }
      }

      iam_role_additional_policies = {
        AmazonS3FullAccess = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
      }
    }
  }
}

variable "env" { type = string }
variable "vpc_id" { type = string }
variable "private_subnet_ids" { type = list(string) }
variable "cluster_version" { type = string }
variable "gpu_instance_types" { type = list(string) }
variable "gpu_min_size" { type = number }
variable "gpu_max_size" { type = number }

output "cluster_name" { value = module.eks.cluster_name }
output "cluster_endpoint" { value = module.eks.cluster_endpoint }
output "cluster_ca_certificate" { value = module.eks.cluster_certificate_authority_data }
output "cluster_security_group_id" { value = module.eks.cluster_security_group_id }
output "oidc_provider_arn" { value = module.eks.oidc_provider_arn }
