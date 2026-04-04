env                 = "prod"
aws_region          = "ap-northeast-1"
vpc_cidr            = "10.1.0.0/16"
eks_cluster_version = "1.30"
gpu_instance_types  = ["g5.xlarge", "g5.2xlarge"]
gpu_min_size        = 1
gpu_max_size        = 4
