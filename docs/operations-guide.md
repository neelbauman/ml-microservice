# ML Pipeline with Dapr — 運用ガイド

> 対象読者: パイプラインを日常的に運用・検証するエンジニア
> 最終更新: 2026-04

---

## 1. 全体アーキテクチャ

本パイプラインは **オンライン (リアルタイム推論)** と **オフライン (モデル学習)** の 2 系統で構成される。

```
┌─────────────────────────────────────────────────────────────────┐
│  オフライン: 学習ワークフロー (Training Service + Prefect)       │
│                                                                 │
│  [S3/MinIO ファイル配置] ──▶ [training-data-events] ──▶ Training │
│  [手動 POST /trigger]    ──▶                            Service │
│  [スケジュール]          ──▶                              │     │
│                                                           ▼     │
│            Prefect Flow: preprocess → validate → train           │
│                          → evaluate → register → MLflow          │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  オンライン: 推論パイプライン (Dapr pub/sub で自動連鎖)         │
│                                                                 │
│  データ投入                                                     │
│      │                                                          │
│      ▼                                                          │
│  Ingestion ──publish──▶ [raw-data]                              │
│                              │                                  │
│                              ▼                                  │
│                       Preprocessing ──publish──▶ [preprocessed] │
│                                                      │          │
│                                                      ▼          │
│                                                 Inference       │
│                                                   │    │        │
│                                            結果保存  異常検知   │
│                                                        │        │
│                                                        ▼        │
│                                           Alert (通知・記録)    │
└─────────────────────────────────────────────────────────────────┘
```

### サービス一覧

| サービス      | ポート | 役割                                               |
|---------------|--------|----------------------------------------------------|
| Ingestion     | 8001   | センサーデータ受信、`raw-data` トピックへ publish  |
| Preprocessing | 8002   | 正規化・特徴量抽出、`preprocessed-data` へ publish |
| Inference     | 8003   | 異常スコア算出、閾値超過時に `alerts` へ publish   |
| Alert         | 8004   | アラート記録・通知                                 |
| Dashboard     | 8005   | リアルタイム UI (SSE)                              |
| Training      | 8006   | 学習ワークフロー (Prefect + Dapr トリガー)        |

### インフラ一覧

| コンポーネント | ポート                | 役割                                       |
|----------------|-----------------------|--------------------------------------------|
| Redpanda       | 19092 (Kafka API)     | メッセージブローカー (Kafka 互換)          |
| Valkey         | 6379                  | ステートストア (Redis 互換)                |
| PostgreSQL     | 5432                  | MLflow バックエンドDB                      |
| MinIO          | 9000 / 9001 (Console) | S3 互換オブジェクトストレージ              |
| MLflow         | 5001                  | 実験追跡・モデルレジストリ                 |
| Prefect Server | 4200                  | ワークフロー管理 UI・API                   |
| Prometheus     | 9090                  | メトリクス収集                             |
| Grafana        | 3000                  | ダッシュボード (初期ログイン: admin/admin) |

---

## 2. 環境の起動と停止

### 2.1 初回セットアップ

```bash
make setup          # uv workspace の全パッケージをインストール
cp .env.example .env  # 環境変数テンプレートをコピー
```

`.env` はプロジェクトルートに配置し、以下の用途で自動的に読み込まれる:

- **docker compose** — コンテナの環境変数として参照 (`${VAR:-default}` 形式)
- **Makefile** — `make train` 等のローカル実行時に `export` される

`.env` に含まれる主な設定:

| 変数                                | デフォルト              | 用途                  |
|-------------------------------------|-------------------------|-----------------------|
| `AWS_ACCESS_KEY_ID`                 | `minioadmin`            | MinIO / S3 認証       |
| `AWS_SECRET_ACCESS_KEY`             | `minioadmin`            | MinIO / S3 認証       |
| `MLFLOW_S3_ENDPOINT_URL`            | `http://localhost:9000` | MLflow → MinIO 接続先 |
| `MLFLOW_TRACKING_URI`               | `http://localhost:5001` | MLflow サーバーの URL |
| `MLFLOW_EXPERIMENT_NAME`            | `anomaly-detection`     | MLflow 実験名         |
| `POSTGRES_DB` / `USER` / `PASSWORD` | `mlflow`                | PostgreSQL 接続情報   |
| `MINIO_ROOT_USER` / `PASSWORD`      | `minioadmin`            | MinIO 管理者認証      |

> **注意**: `.env` は `.gitignore` に含まれるため Git には追跡されない。`.env.example` がテンプレートとしてコミットされている。

### 2.2 起動

```bash
make up             # 全コンテナを起動 (docker compose up -d)
```

起動後、全サービスが ready になるまで 15〜30 秒程度かかる。
ヘルスチェックで確認:

```bash
make health         # 全サービス + Dapr サイドカーの死活確認
make status         # docker compose ps の整形出力
```

### 2.3 停止

```bash
make down           # コンテナ停止 (データボリュームは保持)
make clean          # コンテナ停止 + 全ボリューム削除 (完全リセット)
```

### 2.4 インフラだけ起動

サービスのコードをホスト側で直接実行したい場合:

```bash
make up-infra       # Redpanda, Valkey, PostgreSQL, MinIO, MLflow, Dapr Placement のみ起動
make run-ingestion  # ホスト側で ingestion を hot-reload 起動 (port 8000)
```

---

## 3. オンラインパイプラインの運用

### 3.1 データフロー

データを Ingestion サービスに投入すると、Dapr pub/sub (Redpanda) を介して全ステージが自動的に連鎖実行される。手動で中間サービスを呼び出す必要はない。

```
POST /ingest ──▶ raw-data ──▶ /process ──▶ preprocessed-data ──▶ /predict ──▶ alerts ──▶ /handle-alert
  (Ingestion)                 (Preprocessing)                    (Inference)             (Alert)
```

### 3.2 データ投入

#### サンプルデータの投入

```bash
make seed                    # 12 件投入 (正常 ~85%, 異常 ~15%)
make seed-continuous         # 1 秒に 1 件ずつ連続投入 (Ctrl+C で停止)
```

オプション:

```bash
uv run python scripts/seed_data.py --count 50              # 件数指定
uv run python scripts/seed_data.py --anomaly-rate 0.3      # 異常率 30%
uv run python scripts/seed_data.py --continuous --interval 0.5  # 0.5 秒間隔
```

#### API で直接投入

```bash
# 単件
curl -X POST http://localhost:8001/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "sensor_id": "sensor-A",
    "timestamp": "2026-04-04T12:00:00Z",
    "values": {"temperature": 25.0, "humidity": 60.0, "pressure": 1013.0, "vibration": 0.5},
    "metadata": {"location": "zone-1"}
  }'

# バッチ
curl -X POST http://localhost:8001/ingest/batch \
  -H "Content-Type: application/json" \
  -d '[
    {"sensor_id": "sensor-A", "timestamp": "2026-04-04T12:00:00Z", "values": {"temperature": 25.0}},
    {"sensor_id": "sensor-B", "timestamp": "2026-04-04T12:00:01Z", "values": {"temperature": 80.0}}
  ]'
```

#### データモデル (SensorData)

| フィールド | 型 | 必須 | 説明 |
|-----------|---|------|------|
| `sensor_id` | string | Yes | センサー識別子 |
| `timestamp` | ISO 8601 datetime | No (自動付与) | 計測タイムスタンプ |
| `values` | `dict[string, float]` | Yes | 計測値 (キーは任意) |
| `metadata` | `dict[string, string]` | No | 付加情報 |

### 3.3 結果の確認

#### リアルタイムモニター

```bash
make watch          # 2 秒ごとに全センサーの推論結果を表示
```

出力例:

```
=== ML Pipeline Monitor ===

Sensor         Score  Anomaly Confidence        Model
--------------------------------------------------------
sensor-A      0.2345       no     0.9800   zscore-v1
sensor-B      0.9123      YES     0.7600   zscore-v1
sensor-C      0.1502       no     0.9900   zscore-v1

Alerts: 3 total, 1 critical, 2 warning
```

#### API での個別確認

```bash
# 特定センサーの最新推論結果
curl http://localhost:8003/results/sensor-A

# 最近のアラート一覧 (デフォルト 20 件)
curl http://localhost:8004/alerts/recent?limit=10

# アラート統計
curl http://localhost:8004/alerts/stats
```

### 3.4 異常検知ロジック

推論サービスは 2 つのモードで動作する:

**ONNX モデル (学習済み)** — `make train-all` でモデルを登録すると、Dapr pub/sub 経由で自動デプロイされる:

- Autoencoder の再構成誤差 (MSE) を異常スコアとして使用
- 学習時の `anomaly_threshold` に基づきスコアを 0〜1 に正規化
- モデルのバージョンは `GET /model/status` で確認可能

**Z-score フォールバック** — モデル未ロード時に自動的に使用される:

- 特徴量ベクトルの各要素の Z-score を計算
- 最大の |Z-score| / 3.0 をスコア (0〜1 に正規化) とする

いずれのモードでも:
- **閾値 0.85 超** で異常と判定
- スコア **0.95 超** は CRITICAL、それ以下は WARNING レベルのアラート

### 3.5 モデルの自動デプロイ

```
make train-all
    │
    ├─ preprocess → train → evaluate
    │
    └─ register
         │ MLflow に ONNX モデル登録
         │ model-updates トピックにイベント publish
         ▼
    Inference サービス
         │ イベント受信
         │ MLflow からモデルダウンロード
         ▼
    onnxruntime で推論開始 (ホットスワップ)
```

`make train-register` (または `make train-all`) を実行するだけで、学習済みモデルが推論サービスに自動でデプロイされる。サービスの再起動は不要。

モデルの状態確認:

```bash
curl http://localhost:8003/model/status
# {"loaded":true,"model_name":"anomaly-detector","model_version":"v1.0.0","input_dim":8,"anomaly_threshold":0.05}
```

### 3.6 デモの一括実行

```bash
make demo           # 起動 → 15 秒待機 → 20 件投入 → モニター開始
```

---

## 4. オフラインパイプラインの運用 (モデル学習)

### 4.1 学習パイプラインの全体フロー

```
preprocess → validate → train → evaluate → register → 推論サービスへ自動デプロイ
    │            │         │         │          │              │
    │            │         │         │          │              └─ Dapr pub/sub で通知
    │            │         │         │          └─ MLflow Model Registry に登録
    │            │         │         └─ AUC-ROC, F1 等を計算・記録
    │            │         └─ Autoencoder を学習、MLflow に記録
    │            └─ データ品質チェック (NaN/Inf, 最小サンプル数)
    └─ 合成学習データ生成 / S3 からダウンロード
```

`register` ステップの完了時、Dapr pub/sub (`model-updates` トピック) を通じて推論サービスに自動通知される。推論サービスは MLflow から ONNX モデルをダウンロードし、onnxruntime でホットスワップする。

### 4.2 学習の実行方法

学習パイプラインには **3 つの実行方法** がある。

#### 方法 A: Makefile で手動実行 (従来方式)

```bash
make train-preprocess   # Step 1: 学習データ生成 (合成データ)
make train              # Step 2: モデル学習 (Autoencoder)
make train-evaluate     # Step 3: 評価 (AUC-ROC, F1 等)
make train-register     # Step 4: MLflow に登録 + 推論サービスへ自動デプロイ

make train-all          # 上記 4 ステップを一括実行
```

#### 方法 B: Training Workflow Service 経由 (Prefect)

Training Service は Prefect ベースのワークフローサービスで、イベント駆動で学習パイプラインを実行する。

```bash
# 手動トリガー (フルパイプライン)
make train-trigger
# → POST http://localhost:8006/trigger/full

# S3/MinIO 上のデータを指定して再学習
make train-trigger-s3 S3_PATH=training/2024-01/
# → POST http://localhost:8006/trigger with s3_key

# curl で直接トリガー
curl -X POST http://localhost:8006/trigger \
  -H "Content-Type: application/json" \
  -d '{"source": "manual", "model_version": "v2.0.0"}'
```

#### 方法 C: ファイル配置による自動トリガー

MinIO (ローカル) / S3 (AWS) にデータファイルを配置すると、自動的に学習パイプラインが起動する。

```
[ローカル]
MinIO にファイルアップロード (.npy/.csv/.parquet)
  → MinIO Bucket Notification → Redpanda (training-data-events topic)
  → Dapr subscription → Training Service
  → Prefect Flow 自動起動

[AWS]
S3 PutObject → EventBridge → MSK (training-data-events topic)
  → Dapr subscription → Training Service
  → Prefect Flow 自動起動
```

MinIO Console (`http://localhost:9001`) で `ml-data` バケットにファイルをアップロードするか、`mc` コマンドでアップロードする:

```bash
# mc (MinIO Client) でアップロード
mc alias set local http://localhost:9000 minioadmin minioadmin
mc cp train_data.npy local/ml-data/training/
# → training-data-events トピックにイベントが発行され、学習が自動開始
```

### 4.3 Training Workflow Service の管理

#### Prefect UI

http://localhost:4200 で Prefect ダッシュボードにアクセスできる。

- **Flow Runs** タブ: 実行中・完了済み・失敗したフローの一覧
- 各フローランのタスク実行状況、ログ、所要時間を確認可能
- 失敗したフローの詳細なエラーログとリトライ操作

#### サービスの起動

```bash
# Docker Compose で起動 (make up に含まれる)
make up

# ホスト側で直接起動 (ホットリロード)
make run-training
```

#### トリガー API

| エンドポイント | メソッド | 説明 |
|--------------|---------|------|
| `/trigger` | POST | `TrainingTriggerEvent` を送信してトリガー |
| `/trigger/full` | POST | フルパイプラインを即座にトリガー |
| `/events/training-data` | POST | Dapr pub/sub 経由の S3 イベント受信 (自動) |
| `/health` | GET | ヘルスチェック |

### 4.4 ハイパーパラメータの調整

環境変数で制御可能:

| 環境変数 | デフォルト | 説明 |
|---------|-----------|------|
| `NUM_NORMAL_SAMPLES` | 100000 | 正常データの生成件数 |
| `NUM_ANOMALY_SAMPLES` | 200 | 異常データの生成件数 |
| `NUM_FEATURES` | 8 | 特徴量の次元数 |
| `EPOCHS` | 50 | 学習エポック数 |
| `BATCH_SIZE` | 64 | バッチサイズ |
| `LEARNING_RATE` | 1e-3 | 学習率 |
| `LATENT_DIM` | 3 | Autoencoder の潜在次元 |
| `MODEL_VERSION` | v1.0.0 | モデルバージョン文字列 |
| `ANOMALY_THRESHOLD` | 0.05 | 評価時の異常判定閾値 (再構成誤差) |

使用例:

```bash
# Makefile 経由
EPOCHS=100 BATCH_SIZE=128 LEARNING_RATE=0.0005 MODEL_VERSION=v2.0.0 \
  make train

# Training Service 経由 (環境変数は docker-compose.yml で設定)
```

### 4.5 学習結果の確認

MLflow UI: http://localhost:5001

- **Experiments** タブ: `anomaly-detection` 実験のラン一覧
- 各ランで `train_loss` のエポックごとの推移を確認可能
- 評価ラン (`eval-*`) に AUC-ROC, Precision, Recall, F1 が記録される
- 登録ラン (`register-*`) で ONNX モデルがアーティファクトとして保存される

Prefect UI: http://localhost:4200

- **Flow Runs** タブ: ワークフロー実行履歴
- 各タスク (preprocess, validate, train, evaluate, register) の成功/失敗と所要時間

### 4.6 本番環境での学習

本番では Training Service が EKS 上で常駐し、S3 イベント → MSK → Dapr で自動トリガーされる。

Argo Workflows による手動実行も引き続き利用可能:

```bash
argo submit ml/training/argo-workflow.yaml \
  -p model-version=v2.0.0 \
  -p s3-data-path=s3://ml-data/train/ \
  -p experiment-name=anomaly-detection
```

DAG 構成: `preprocess → train (GPU) → evaluate → register`

---

## 5. 監視とトラブルシューティング

### 5.1 ログの確認

```bash
make logs                # 全サービスのログを tail
make logs-ingestion      # 個別サービス (ingestion / preprocessing / inference / alert)
make logs-inference      # 例: inference のログ
```

### 5.2 監視ダッシュボード

| ツール | URL | 用途 |
|--------|-----|------|
| Grafana | http://localhost:3000 | メトリクスダッシュボード |
| Prometheus | http://localhost:9090 | メトリクスクエリ |
| MLflow | http://localhost:5001 | 学習実験・モデル管理 |
| Prefect | http://localhost:4200 | 学習ワークフロー管理 |
| MinIO Console | http://localhost:9001 | オブジェクトストレージ (minioadmin/minioadmin) |
| Redpanda Console | http://localhost:19644 | ブローカーステータス |

### 5.3 Kafka トピックの確認

```bash
# トピック一覧
docker compose exec redpanda rpk topic list

# トピックの手動作成 (通常は auto-create で不要)
make create-topics

# トピックのメッセージを読む
docker compose exec redpanda rpk topic consume raw-data --num 5
docker compose exec redpanda rpk topic consume preprocessed-data --num 5
docker compose exec redpanda rpk topic consume inference-results --num 5
docker compose exec redpanda rpk topic consume alerts --num 5
docker compose exec redpanda rpk topic consume training-data-events --num 5
```

### 5.4 ステートストアの確認

Dapr ステートストア (Valkey) に保存された最新状態:

```bash
# Valkey に直接接続して確認
docker compose exec valkey valkey-cli

# キーの一覧
> KEYS *

# 特定キーの値を確認
> GET "ingestion||ingestion:last:sensor-A"
> GET "inference||inference:last:sensor-A"
> GET "alert||alert:last:sensor-A"
```

### 5.5 よくあるトラブル

| 症状 | 原因 | 対処 |
|------|------|------|
| `[x]` で seed が失敗 | Dapr sidecar → Redpanda の接続失敗 | `make health` で Dapr sidecar の状態を確認。Redpanda の `advertised_kafka_api` が `redpanda:9092` になっているか確認 |
| `[!] Connection refused` | サービスが未起動 | `make up` で起動、`make status` で確認 |
| Preprocessing が動かない | Dapr subscription が未登録 | `curl http://localhost:8002/dapr/subscribe` でルート確認。sidecar を再起動 |
| 推論結果が `no data` | データがまだ流れていない | `make seed` でデータ投入 |
| MLflow に接続できない | PostgreSQL / MinIO 未起動 | `make up-infra` でインフラ起動、`make health` で確認 |
| `NoCredentialsError` (学習時) | MinIO 認証情報が未設定 | `.env` が存在するか確認。`cp .env.example .env` で作成 |
| `model_load_failed` (推論) | inference → MLflow 接続失敗 | `docker compose logs inference` で詳細確認。MLflow が起動しているか `make health` で確認 |
| `model_update_publish_failed` | register.py → Dapr sidecar 接続失敗 | `make up` でサービスが起動しているか確認。`.env` の `DAPR_HTTP_PORT=3501` を確認 |
| コンテナの状態がおかしい | ボリュームの不整合 | `make clean && make up` で完全リセット |
| ファイル配置しても学習が始まらない | MinIO Bucket Notification 未設定 | `docker compose logs minio-notify-init` で設定ログ確認。`make clean && make up` で再設定 |
| Prefect UI にフローが表示されない | Prefect Server 未起動 | `docker compose logs prefect-server` で確認。`PREFECT_API_URL` が正しいか確認 |
| 学習トリガー後にフローが失敗 | MLflow/MinIO 接続エラー | `docker compose logs training` でエラー確認。`make health` でインフラ状態確認 |

---

## 6. コマンドリファレンス

### 環境管理

| コマンド | 説明 |
|---------|------|
| `make setup` | 全パッケージインストール |
| `make up` | 全コンテナ起動 |
| `make up-infra` | インフラのみ起動 |
| `make down` | コンテナ停止 |
| `make clean` | コンテナ停止 + ボリューム削除 |
| `make status` | コンテナ状態確認 |
| `make health` | 全ヘルスチェック |

### オンラインパイプライン

| コマンド | 説明 |
|---------|------|
| `make seed` | サンプルデータ投入 (12 件) |
| `make seed-continuous` | 連続データ投入 (1/sec) |
| `make watch` | リアルタイムモニター |
| `make demo` | 起動→投入→監視を一括実行 |

### モデル学習

| コマンド | 説明 |
|---------|------|
| `make train-preprocess` | 学習データ生成 |
| `make train` | モデル学習 |
| `make train-evaluate` | モデル評価 |
| `make train-register` | MLflow に登録 + 推論サービスへ自動デプロイ |
| `make train-all` | 上記 4 ステップを一括実行 |
| `make run-training` | 学習ワークフローサービスをホットリロード起動 |
| `make train-trigger` | ワークフローサービス経由で学習トリガー |
| `make train-trigger-s3` | S3 パス指定で再学習トリガー (`S3_PATH=...`) |

### 開発・運用

| コマンド | 説明 |
|---------|------|
| `make logs` | 全ログ tail |
| `make logs-<service>` | 個別サービスのログ |
| `make test` | 全テスト実行 |
| `make lint` | lint チェック |
| `make build` | 全 Docker イメージビルド |
| `make create-topics` | Kafka トピック手動作成 |
| `make run-<service>` | ホスト側で個別サービスを hot-reload 起動 |
