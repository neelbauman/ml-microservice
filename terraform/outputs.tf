output "eks_cluster_name" {
  value = module.eks.cluster_name
}

output "eks_cluster_endpoint" {
  value = module.eks.cluster_endpoint
}

output "msk_bootstrap_brokers" {
  value     = module.msk.bootstrap_brokers_iam
  sensitive = true
}

output "elasticache_endpoint" {
  value = module.elasticache.primary_endpoint
}

output "aurora_endpoint" {
  value = module.aurora.cluster_endpoint
}

output "s3_model_bucket" {
  value = module.s3.model_bucket_name
}

output "s3_data_bucket" {
  value = module.s3.data_bucket_name
}

output "amp_workspace_id" {
  value = module.monitoring.amp_workspace_id
}
