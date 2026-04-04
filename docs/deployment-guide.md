# ML Pipeline with Dapr — デプロイガイド

> 対象読者: インフラ担当者、SRE、デプロイ権限を持つエンジニア
> 最終更新: 2026-04

---

## 1. はじめに

本ドキュメントは、ML 推論パイプラインの AWS 本番環境へのデプロイ手順を記述する。

デプロイは以下の 2 トラックで構成される。

- **インフラトラック**: Terraform で AWS リソースを構築 (EKS, MSK, ElastiCache, Aurora, S3, AMP/AMG)
- **CI/CD トラック**: GitHub Actions でコード検証 → Docker ビルド → EKS デプロイ

インフラトラックを先に実行し、EKS クラスタが稼働した状態で CI/CD トラックがアプリケーションをデプロイする。


---

## 2. アーキテクチャ概要

### 2.1 AWS 構成

```
Internet
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│ AWS VPC (ap-northeast-1)                                     │
│                                                              │
│  ┌─ ALB (Ingress) ──────────────────────────────────────┐   │
│  └──────────────────────────────────────────────────────┘   │
│                          │                                   │
│  ┌─ Amazon EKS ─────────┼──────────────────────────────┐   │
│  │                       ▼                              │   │
│  │  ┌─ System Nodes ──────────────────────────┐        │   │
│  │  │  Dapr, ArgoCD, OTel Collector           │        │   │
│  │  └─────────────────────────────────────────┘        │   │
│  │  ┌─ App Nodes ─────────────────────────────┐        │   │
│  │  │  FastAPI services, MLflow, Prefect, Feast│        │   │
│  │  └─────────────────────────────────────────┘        │   │
│  │  ┌─ GPU Nodes (g5.xlarge) ─────────────────┐        │   │
│  │  │  Training Jobs, Triton Inference         │        │   │
│  │  └─────────────────────────────────────────┘        │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  ┌─ Managed Services ──────────────────────────────────┐    │
│  │  Amazon MSK (Kafka)  │  ElastiCache (Valkey)        │    │
│  │  Aurora PostgreSQL   │                               │    │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘

Regional Services: Amazon S3, ECR, AMP, AMG
```


### 2.2 EKS ノードグループ

| ノードグループ | インスタンス | 用途 | dev | prod |
|--------------|------------|------|-----|------|
| system | m6i.large | Dapr control plane, ArgoCD, OTel | 2台 固定 | 2-4台 |
| app | c5.xlarge | FastAPI services, MLflow, Feast | 2-10台 | 2-10台 (HPA) |
| gpu | g5.xlarge | Training, Triton Inference | 0-2台 | 1-4台 |

GPU ノードには `nvidia.com/gpu=true:NoSchedule` の taint が設定されており、GPU を必要としないワークロードは配置されない。


### 2.3 Dapr コンポーネントの環境マッピング

| コンポーネント名 | ローカル実体 | AWS 実体 | 認証方式 |
|----------------|-----------|---------|---------|
| `pubsub` | Redpanda | Amazon MSK | IAM |
| `statestore` | Valkey | ElastiCache | TLS + パスワード |
| `model-store` | MinIO | Amazon S3 | IRSA |

アプリケーションコードはコンポーネント名のみを参照し、環境を意識しない。


---

## 3. 前提条件

### 3.1 必要なツール

| ツール | バージョン | 用途 |
|--------|----------|------|
| aws cli | v2 | AWS 操作 |
| terraform | 1.9 以上 | インフラ構築 |
| kubectl | 1.30 以上 | EKS 操作 |
| helm | 3.16 以上 | K8s デプロイ |
| docker | 最新 | イメージビルド |
| uv | 0.5 以上 | Python 依存管理 |

### 3.2 AWS アカウント要件

- 適切な IAM ロール (EKS, MSK, ElastiCache, Aurora, S3, AMP, AMG の作成権限)
- Terraform state 用の S3 バケット + DynamoDB テーブル（事前作成が必要）
- ECR リポジトリ（CI/CD が自動作成、または事前作成）
- GitHub Actions 用の OIDC プロバイダ設定


---

## 4. インフラトラック — Terraform

### 4.1 初回構築手順

#### Step 1: Terraform state バックエンドの準備

```bash
# S3 バケットと DynamoDB テーブルを作成 (初回のみ)
aws s3 mb s3://ml-pipeline-tfstate-dev --region ap-northeast-1
aws dynamodb create-table \
  --table-name ml-pipeline-tflock-dev \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region ap-northeast-1
```

#### Step 2: Terraform 初期化

```bash
make tf-init ENV=dev
```

内部で実行される処理:

```bash
cd terraform && terraform init -backend-config=envs/dev/backend.hcl
```

参照ファイル:

| ファイル | 内容 |
|----------|------|
| `terraform/envs/dev/backend.hcl` | S3 バケット名、DynamoDB テーブル名、リージョン |
| `terraform/envs/dev/terraform.tfvars` | 環境固有のパラメータ |


#### Step 3: プラン確認

```bash
make tf-plan ENV=dev
```

作成されるリソースを確認する。主要なリソースと参照ファイルは以下の通り。

| Terraform モジュール | 作成リソース | 参照ファイル |
|---------------------|------------|------------|
| `modules/vpc` | VPC, 3 AZ × (Public + Private) Subnet, NAT Gateway | `modules/vpc/main.tf` |
| `modules/eks` | EKS クラスタ, 3 ノードグループ (system/app/gpu) | `modules/eks/main.tf` |
| `modules/eks/addons` | Dapr 1.14, ArgoCD, Argo Workflows, OTel Collector, NVIDIA Plugin | `modules/eks/addons.tf` |
| `modules/msk` | MSK クラスタ (Kafka 3.6, IAM 認証, TLS) | `modules/msk/main.tf` |
| `modules/elasticache` | ElastiCache Replication Group (Valkey 8.0) | `modules/elasticache/main.tf` |
| `modules/aurora` | Aurora PostgreSQL Serverless v2 | `modules/aurora/main.tf` |
| `modules/s3` | S3 バケット × 3 (models, data, mlflow-artifacts) | `modules/s3/main.tf` |
| `modules/monitoring` | Amazon Managed Prometheus + Managed Grafana | `modules/monitoring/main.tf` |


#### Step 4: 適用

```bash
make tf-apply
```

所要時間の目安:
- EKS クラスタ: 10-15 分
- MSK: 15-20 分
- その他: 3-5 分
- 合計: 約 30-40 分


#### Step 5: kubeconfig 設定

```bash
aws eks update-kubeconfig \
  --name ml-pipeline-dev \
  --region ap-northeast-1
```

#### Step 6: Dapr 動作確認

```bash
# Dapr がインストールされていることを確認
kubectl get pods -n dapr-system

# ArgoCD の確認
kubectl get pods -n argocd

# Argo Workflows の確認
kubectl get pods -n argo
```


### 4.2 環境別パラメータ

| パラメータ | dev | prod |
|----------|-----|------|
| `vpc_cidr` | 10.0.0.0/16 | 10.1.0.0/16 |
| `gpu_instance_types` | ["g5.xlarge"] | ["g5.xlarge", "g5.2xlarge"] |
| `gpu_min_size` | 0 | 1 |
| `gpu_max_size` | 2 | 4 |
| ElastiCache ノード数 | 1 | 3 (Multi-AZ) |
| Aurora インスタンス数 | 1 | 2 (Writer + Reader) |
| NAT Gateway | Single | Per-AZ |
| S3 暗号化 | SSE-KMS | SSE-KMS |
| Aurora 削除保護 | 無効 | 有効 |


### 4.3 Terraform 出力値

`terraform output` で以下の値が取得できる。これらは Helm values や Dapr コンポーネント設定で使用する。

| 出力名 | 用途 |
|--------|------|
| `eks_cluster_name` | kubeconfig 設定 |
| `eks_cluster_endpoint` | Helm/kubectl の接続先 |
| `msk_bootstrap_brokers` | Dapr Pub/Sub 設定 |
| `elasticache_endpoint` | Dapr State Store 設定 |
| `aurora_endpoint` | MLflow バックエンド |
| `s3_model_bucket` | Dapr Binding / モデル保存先 |
| `amp_workspace_id` | Prometheus remote write 先 |


---

## 5. CI/CD トラック — GitHub Actions

### 5.1 パイプライン全体像

```
git push (main)
    │
    ├──▶ ci.yml (自動: main push / PR to main, dev)
    │     ├── lint:    uv run ruff check/format
    │     ├── test:    uv run pytest (Valkey コンテナ付き)
    │     ├── helm:    helm lint + template (dev/prod)
    │     └── terraform: fmt + validate
    │
    ├──▶ build.yml (source/services/ source/libs/ source/ml/ 変更時)
    │     ├── detect-changes: paths-filter で変更検出
    │     ├── build-services: 変更サービスのみ Docker → ECR (GHA キャッシュ付き)
    │     └── build-training-gpu: source/ml/ 変更時のみ GPU イメージ → ECR
    │
    ├──▶ deploy.yml (build 完了後 or 手動)
    │     ├── kubeconfig: aws eks update-kubeconfig
    │     ├── helm: helm upgrade --install -f values-${ENV}.yaml
    │     └── verify: kubectl rollout status (全 6 サービス)
    │
    └──▶ security.yml (PR / main push / 週次月曜 03:00 UTC)
          ├── dependency-audit: pip-audit で Python 依存の CVE スキャン
          └── trivy-scan: 全 6 サービスのコンテナイメージ脆弱性スキャン (CRITICAL/HIGH)
```


### 5.2 CI パイプライン (`ci.yml`)

トリガー: `main` への push / `main` または `dev` への Pull Request

| ジョブ | 実行内容 | 失敗時の影響 |
|--------|---------|------------|
| lint | `uv run ruff check .` + `format --check` | PR マージ不可 |
| test | `uv sync --all-packages --dev` → `uv run pytest` | PR マージ不可 |
| helm-validate | `helm lint` + `helm template` (dev/prod) | PR マージ不可 |
| terraform-validate | `terraform fmt -check` + `validate` | PR マージ不可 |

uv のセットアップには `astral-sh/setup-uv@v4` を使用する。`enable-cache: true` により依存キャッシュが効く。


### 5.3 Build パイプライン (`build.yml`)

トリガー: `main` への push (対象パス変更時のみ)

変更検出ルール:

| サービス | 再ビルドトリガー |
|---------|-----------------|
| ingestion | `source/services/ingestion/**` または `source/libs/common/**` |
| preprocessing | `source/services/preprocessing/**` または `source/libs/common/**` |
| inference | `source/services/inference/**` または `source/libs/common/**` |
| alert | `source/services/alert/**` または `source/libs/common/**` |
| dashboard | `source/services/dashboard/**` または `source/libs/common/**` |
| training | `source/services/training/**` または `source/ml/training/**` または `source/libs/common/**` |
| training-gpu | `source/ml/training/**` または `source/libs/common/**` |

`source/libs/common` の変更は全サービスの再ビルドをトリガーする。これは共有ライブラリの変更が全サービスに影響するためである。

Docker ビルドの特徴:
- ビルドコンテキストはリポジトリルート (`context: .`) — uv workspace の `pyproject.toml` と `uv.lock` にアクセスするため
- `uv sync --frozen` により `uv.lock` の内容が厳密に再現される
- 依存インストールとソースコピーを分離し、Docker レイヤーキャッシュを最大化
- GitHub Actions cache (`cache-from: type=gha`) によりビルドレイヤーをジョブ間で再利用
- ECR には `latest` と `${github.sha}` の 2 タグで push


### 5.4 Security パイプライン (`security.yml`)

トリガー: `main` / `dev` への PR・push、週次月曜 03:00 UTC (スケジュール)

| ジョブ | 実行内容 | 失敗時の影響 |
|--------|---------|------------|
| dependency-audit | `pip-audit --strict --desc` で全 Python 依存の CVE チェック | PR マージ不可 |
| trivy-scan | 全 6 サービスの Docker イメージを Trivy でスキャン (CRITICAL/HIGH) | PR マージ不可 |

Trivy の結果は SARIF 形式で GitHub Security タブに自動アップロードされる。週次スケジュールにより、新たに発見された脆弱性も定期的にキャッチできる。


### 5.5 Deploy パイプライン (`deploy.yml`)

トリガー: Build パイプライン完了後 (自動) / 手動 (workflow_dispatch)

```bash
helm upgrade --install ml-pipeline ./helm \
  -f helm/values-${ENV}.yaml \
  --namespace ml-pipeline \
  --create-namespace \
  --set global.imageRegistry=${ECR_REGISTRY} \
  --wait --timeout 10m
```

デプロイ後の検証では全 6 サービスの rollout status を確認する:

```bash
kubectl -n ml-pipeline rollout status deployment/ingestion --timeout=300s
kubectl -n ml-pipeline rollout status deployment/preprocessing --timeout=300s
kubectl -n ml-pipeline rollout status deployment/inference --timeout=300s
kubectl -n ml-pipeline rollout status deployment/alert --timeout=300s
kubectl -n ml-pipeline rollout status deployment/dashboard --timeout=300s
kubectl -n ml-pipeline rollout status deployment/training --timeout=300s
```

Helm が参照する主要な設定:

| values 項目 | dev | prod |
|------------|-----|------|
| `services.ingestion.replicas` | 1 | 3 |
| `services.ingestion.hpa.enabled` | false | true (2-10, CPU 70%) |
| `services.inference.replicas` | 1 | 2 |
| `services.inference.hpa.enabled` | false | true (2-8, カスタムメトリクス) |
| `services.inference.nodeSelector` | なし | `role: gpu` |
| `services.inference.tolerations` | なし | `nvidia.com/gpu: NoSchedule` |
| `dapr.components.pubsub.brokers` | — | MSK エンドポイント |
| `dapr.components.pubsub.authType` | none | iam |
| `dapr.components.statestore.enableTLS` | false | true |
| `monitoring.prometheus.remoteWrite.enabled` | false | true (AMP) |

Dapr sidecar は Deployment の annotation (`dapr.io/enabled: "true"`) により自動注入される。`helm/templates/deployment.yaml` の Pod template に annotation が定義されている。


### 5.6 GitHub Actions の必要シークレット

| シークレット名 | 内容 |
|--------------|------|
| `AWS_ROLE_ARN` | GitHub Actions 用 IAM ロールの ARN (OIDC) |
| `ECR_REGISTRY` | ECR レジストリ URL |


---

## 6. 手動デプロイ手順

GitHub Actions を使わず、ローカルから直接デプロイする場合の手順。

### 6.1 イメージのビルドと push

```bash
# ECR ログイン
make ecr-login

# 全サービスビルド + push
make build
make push
```

### 6.2 Helm デプロイ

```bash
# dev 環境
make deploy ENV=dev

# prod 環境 (先にドライランで確認)
make deploy-dry ENV=prod
make deploy ENV=prod
```

### 6.3 デプロイ確認

```bash
# Pod の状態確認
kubectl -n ml-pipeline get pods

# 各サービスのログ
kubectl -n ml-pipeline logs -l app=ingestion -c ingestion --tail=50

# Dapr sidecar のログ
kubectl -n ml-pipeline logs -l app=ingestion -c daprd --tail=50

# サービス疎通確認
kubectl -n ml-pipeline port-forward svc/ingestion 8001:80
curl http://localhost:8001/health

# 全サービスの rollout status 一括確認
for svc in ingestion preprocessing inference alert dashboard training; do
  echo "=== $svc ==="
  kubectl -n ml-pipeline rollout status deployment/$svc --timeout=60s
done
```


---

## 7. ML 学習パイプライン

本番では 2 つの方法で学習パイプラインを実行できる。

### 7.1 Training Workflow Service (Prefect — 推奨)

Training Service は EKS の App ノードに常駐し、S3 イベントで自動トリガーされる。

**S3 イベントによる自動トリガー (本番)**:
```
S3 PutObject (.npy/.csv/.parquet)
  → EventBridge → MSK (training-data-events topic)
  → Dapr subscription → Training Service
  → Prefect Flow: preprocess → validate → train → evaluate → register
  → model-updates topic → Inference Service ホットスワップ
```

**手動トリガー**:
```bash
# Training Service API 経由
kubectl -n ml-pipeline port-forward svc/training 8006:80
curl -X POST http://localhost:8006/trigger/full
curl -X POST http://localhost:8006/trigger \
  -H "Content-Type: application/json" \
  -d '{"source": "manual", "s3_key": "training/2024-01/", "model_version": "v2.0.0"}'
```

**Prefect UI**:
```bash
kubectl -n ml-pipeline port-forward svc/prefect-server 4200:4200
open http://localhost:4200
```

### 7.2 Argo Workflows (GPU ジョブ)

GPU を使った大規模学習には引き続き Argo Workflows を利用可能:

```bash
argo submit ml/training/argo-workflow.yaml \
  -p model-version="v1.2.0" \
  -p s3-data-path="s3://ml-data/train/2024-01/" \
  -p experiment-name="experiment-42" \
  -n ml-pipeline
```

### 7.3 DAG 構成

| ステップ | 処理内容 | リソース | 依存 |
|---------|---------|---------|------|
| preprocess | データ前処理 | CPU:2, Mem:4Gi | なし |
| validate | データ品質チェック | CPU:1, Mem:2Gi | preprocess |
| train | PyTorch 学習 | CPU:4, Mem:16Gi, GPU:1 | validate |
| evaluate | モデル評価 + MLflow 記録 | CPU:2, Mem:8Gi | train |
| register | Model Registry 登録 | CPU:1, Mem:2Gi | evaluate |

train ステップは `nodeSelector: {role: gpu}` と GPU toleration により g5 ノードに配置される。

### 7.4 学習ジョブの監視

```bash
# Argo ジョブ一覧
argo list -n ml-pipeline

# Argo ジョブログ
argo logs -n ml-pipeline ml-training-xxxxx

# MLflow UI (ポートフォワード)
kubectl -n ml-pipeline port-forward svc/mlflow 5001:5000
open http://localhost:5001

# Prefect UI (ポートフォワード)
kubectl -n ml-pipeline port-forward svc/prefect-server 4200:4200
open http://localhost:4200
```


---

## 8. 監視とオブザーバビリティ

### 8.1 メトリクス

Dapr sidecar は OpenTelemetry 形式でメトリクスを自動出力する。

ローカル環境: Prometheus (`localhost:9090`) → Grafana (`localhost:3000`)
AWS 環境: OTel Collector → Amazon Managed Prometheus (AMP) → Amazon Managed Grafana (AMG)

主要メトリクス:

| メトリクス名 | 意味 |
|-------------|------|
| `dapr_component_pubsub_ingress_count` | Pub/Sub メッセージ受信数 |
| `dapr_component_pubsub_egress_count` | Pub/Sub メッセージ送信数 |
| `dapr_component_state_count` | State Store 操作数 |
| `dapr_http_server_request_body_size` | サービス呼び出しレイテンシ |


### 8.2 分散トレーシング

Dapr sidecar は OpenTelemetry トレースを自動生成する。`dapr/config.yaml` で `samplingRate: "1"` (全リクエスト) を設定している。

AWS 環境では OTel Collector 経由で X-Ray や Jaeger に送信可能。


### 8.3 ログ

全サービスは structlog で JSON 構造化ログを出力する。

```bash
# EKS でのログ確認
kubectl -n ml-pipeline logs -l app=inference -c inference --tail=100 -f
```


---

## 9. 運用手順

### 9.1 ロールバック

```bash
# Helm リリース履歴の確認
helm -n ml-pipeline history ml-pipeline

# 前のバージョンにロールバック
helm -n ml-pipeline rollback ml-pipeline <REVISION>
```

### 9.2 スケーリング

```bash
# 手動スケール
kubectl -n ml-pipeline scale deployment/ingestion --replicas=5

# HPA 設定確認
kubectl -n ml-pipeline get hpa

# GPU ノードのスケール (Terraform で変更)
# terraform/envs/prod/terraform.tfvars の gpu_max_size を変更
make tf-plan ENV=prod
make tf-apply
```

### 9.3 Dapr コンポーネントの更新

Dapr コンポーネント（Pub/Sub, State Store 等）の設定を変更する場合:

```bash
# 1. dapr/components/aws/ の YAML を編集
# 2. Helm values でコンポーネント設定を更新
# 3. Helm デプロイ
make deploy ENV=prod

# 4. Dapr sidecar の再起動 (Pod の再起動で反映)
kubectl -n ml-pipeline rollout restart deployment/ingestion
```

### 9.4 シークレットの更新

ElastiCache パスワードや Aurora パスワードは Secrets Manager に保存されている。ローテーション手順:

```bash
# 1. Secrets Manager でパスワード更新
aws secretsmanager update-secret \
  --secret-id ml-pipeline/prod/elasticache-password \
  --secret-string "new-password"

# 2. K8s Secret を更新
kubectl -n ml-pipeline create secret generic elasticache-credentials \
  --from-literal=password="new-password" \
  --dry-run=client -o yaml | kubectl apply -f -

# 3. Pod を再起動して反映
kubectl -n ml-pipeline rollout restart deployment --selector=app
```


---

## 10. 環境の破棄

### 10.1 dev 環境の破棄

```bash
# 1. アプリケーションの削除
helm -n ml-pipeline uninstall ml-pipeline

# 2. AWS リソースの破棄
cd terraform
terraform destroy -var-file=envs/dev/terraform.tfvars
```

### 10.2 prod 環境の注意

prod 環境では Aurora に `deletion_protection = true` が設定されている。破棄する場合は先に手動で無効化する必要がある。

```bash
aws rds modify-db-cluster \
  --db-cluster-identifier ml-pipeline-prod \
  --no-deletion-protection
```


---

## 11. チェックリスト

### 11.1 初回デプロイ前チェックリスト

- [ ] AWS アカウントに必要な IAM 権限がある
- [ ] Terraform state 用 S3 + DynamoDB が作成済み
- [ ] GitHub リポジトリに `AWS_ROLE_ARN` と `ECR_REGISTRY` シークレットが設定済み
- [ ] `terraform/envs/{env}/terraform.tfvars` のパラメータを確認
- [ ] `terraform/envs/{env}/backend.hcl` のバケット名を確認

### 11.2 デプロイ後チェックリスト

- [ ] `kubectl -n ml-pipeline get pods` で全 Pod (ingestion, preprocessing, inference, alert, dashboard, training) が Running
- [ ] `kubectl -n ml-pipeline logs -l app=ingestion -c daprd` でエラーなし
- [ ] `kubectl -n ml-pipeline logs -l app=training -c daprd` でエラーなし
- [ ] Grafana ダッシュボードでメトリクスが表示されている
- [ ] `make seed` 相当のテストデータで E2E 動作確認
- [ ] GitHub Security タブで Trivy / pip-audit の結果に CRITICAL/HIGH がないことを確認
