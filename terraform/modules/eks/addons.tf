# Dapr
resource "helm_release" "dapr" {
  name             = "dapr"
  repository       = "https://dapr.github.io/helm-charts/"
  chart            = "dapr"
  version          = "1.14.1"
  namespace        = "dapr-system"
  create_namespace = true

  set {
    name  = "global.mtls.enabled"
    value = "true"
  }
  set {
    name  = "global.logAsJson"
    value = "true"
  }

  depends_on = [module.eks]
}

# ArgoCD
resource "helm_release" "argocd" {
  name             = "argocd"
  repository       = "https://argoproj.github.io/argo-helm"
  chart            = "argo-cd"
  version          = "7.6.1"
  namespace        = "argocd"
  create_namespace = true

  set {
    name  = "server.service.type"
    value = "LoadBalancer"
  }
  set {
    name  = "server.extraArgs[0]"
    value = "--insecure"
  }

  depends_on = [module.eks]
}

# Argo Workflows
resource "helm_release" "argo_workflows" {
  name             = "argo-workflows"
  repository       = "https://argoproj.github.io/argo-helm"
  chart            = "argo-workflows"
  version          = "0.42.3"
  namespace        = "argo"
  create_namespace = true

  set {
    name  = "server.extraArgs[0]"
    value = "--auth-mode=server"
  }

  depends_on = [module.eks]
}

# OpenTelemetry Collector
resource "helm_release" "otel_collector" {
  name             = "otel-collector"
  repository       = "https://open-telemetry.github.io/opentelemetry-helm-charts"
  chart            = "opentelemetry-collector"
  version          = "0.103.0"
  namespace        = "observability"
  create_namespace = true

  values = [<<-YAML
    mode: deployment
    config:
      receivers:
        otlp:
          protocols:
            grpc:
              endpoint: "0.0.0.0:4317"
            http:
              endpoint: "0.0.0.0:4318"
      processors:
        batch:
          timeout: 5s
          send_batch_size: 1024
      exporters:
        prometheusremotewrite:
          endpoint: "${var.amp_remote_write_url}"
          auth:
            authenticator: sigv4auth
        logging:
          loglevel: warn
      extensions:
        sigv4auth:
          region: "${var.aws_region}"
      service:
        extensions: [sigv4auth]
        pipelines:
          metrics:
            receivers: [otlp]
            processors: [batch]
            exporters: [prometheusremotewrite, logging]
          traces:
            receivers: [otlp]
            processors: [batch]
            exporters: [logging]
  YAML
  ]

  depends_on = [module.eks]
}

variable "amp_remote_write_url" {
  type    = string
  default = ""
}

variable "aws_region" {
  type    = string
  default = "ap-northeast-1"
}

# NVIDIA Device Plugin (GPU support)
resource "helm_release" "nvidia_device_plugin" {
  name       = "nvidia-device-plugin"
  repository = "https://nvidia.github.io/k8s-device-plugin"
  chart      = "nvidia-device-plugin"
  version    = "0.16.2"
  namespace  = "kube-system"

  set {
    name  = "tolerations[0].key"
    value = "nvidia.com/gpu"
  }
  set {
    name  = "tolerations[0].operator"
    value = "Exists"
  }
  set {
    name  = "tolerations[0].effect"
    value = "NoSchedule"
  }

  depends_on = [module.eks]
}
