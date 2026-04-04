# ML Pipeline with Dapr — 開発ガイド

> 対象読者: 本プロジェクトで開発を行うエンジニア
> 最終更新: 2026-04

---

## 1. はじめに

本ドキュメントは、Dapr サイドカーパターンによる ML 推論パイプラインのローカル開発手順を網羅的に記述する。

`make up` を実行するだけで、Kafka互換メッセージブローカー、キャッシュ、DB、オブジェクトストレージ、MLflow、Grafana を含む開発環境が立ち上がる。サンプルデータを投入すれば、4つのサービスがパイプラインとしてデータを処理し、異常検知結果がリアルタイムに確認できる。


---

## 2. 前提条件

| ツール | バージョン | 用途 | インストール |
|--------|----------|------|------------|
| uv | 0.5 以上 | Python パッケージ管理 | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| Docker Desktop | 最新 | コンテナ実行 (Compose v2 含む) | https://www.docker.com/products/docker-desktop |
| make | any | コマンドショートカット | OS 標準 |
| git | any | バージョン管理 | OS 標準 |

Docker Desktop のメモリ割り当ては 6GB 以上を推奨する。全コンテナが同時に起動するため、4GB では不安定になる場合がある。


---

## 3. プロジェクト構成

### 3.1 uv Workspace

本プロジェクトは uv workspace によるモノレポ構成を採用している。

```
ml-dapr-pipeline/
├── pyproject.toml           ← ワークスペースルート (dev ツール定義)
├── uv.lock                  ← 全メンバー共通ロックファイル
├── .python-version          ← Python 3.12
│
├── libs/common/             ← 共有ライブラリ
│   ├── pyproject.toml       ← ml-pipeline-common
│   └── src/common/
│       ├── config.py        ← ServiceSettings (pydantic-settings)
│       ├── dapr_helpers.py  ← Dapr HTTP API ラッパー
│       ├── health.py        ← GET /health エンドポイント
│       ├── logging.py       ← structlog セットアップ
│       ├── middleware.py    ← リクエストログ middleware
│       └── models.py       ← SensorData, ProcessedData, InferenceResult, Alert
│
├── services/
│   ├── ingestion/           ← ingestion-svc
│   ├── preprocessing/       ← preprocessing-svc
���   ├── inference/           ← inference-svc
│   ├── alert/               ← alert-svc
│   └── training/            ← training-svc (Prefect ワークフロー)
│
├── ml/training/             ← ml-training (GPU 学習ジョブ)
│
├── dapr/components/
│   ├── local/               ← Redpanda, Valkey, MinIO
│   └── aws/                 ← MSK, ElastiCache, S3
│
├── scripts/                 ← 開発用ツール
├── monitoring/              ← Prometheus + Grafana 設定
├── docs/                    ← Dapr 開発ガイド
├── docker-compose.yml       ← ローカル開発環境定義
└── Makefile                 ← コマンド集
```


### 3.2 ワークスペースメンバーと依存関係

```
ml-pipeline-common (libs/common)
    ├── dapr, fastapi, pydantic, structlog
    │
    ├──▶ ingestion-svc      + httpx
    ├──▶ preprocessing-svc  + numpy, pandas
    ├──▶ inference-svc      + onnxruntime, tritonclient
    ├──▶ alert-svc          (common のみ)
    ├──▶ ml-training        + torch, mlflow, feast
    └──�� training-svc       + ml-training, prefect, prefect-aws
```

全メンバーが `ml-pipeline-common` ��依存しており、Dapr 操作・ログ設定・データモデルを共有する。`training-svc` は `ml-training` にも依存し、既存の学習ロジックを Prefect タスクとして再利用する。開発ツール (pytest, ruff, httpx) はルート `pyproject.toml` に一元定義されている。


### 3.3 データモデル

パイプライン全体で共有される Pydantic モデルは `libs/common/src/common/models.py` に定義されている。

| モデル | 用途 | 主要フィールド |
|--------|------|-------------|
| `SensorData` | 生センサーデータ | sensor_id, timestamp, values (dict), metadata |
| `ProcessedData` | 正規化済みデータ | sensor_id, features (list[float]), feature_names |
| `InferenceResult` | 推論結果 | prediction, confidence, is_anomaly, model_version |
| `Alert` | 異常アラート | alert_id, level (INFO/WARNING/CRITICAL), message |
| `ModelUpdateEvent` | モデル更新通知 | model_name, model_version, model_uri, run_id |
| `TrainingTriggerEvent` | 学習トリガー | source (s3_event/manual/schedule), s3_bucket, s3_key |


---

## 4. 環境セットアップ

### 4.1 初回セットアップ

```bash
git clone <repository-url>
cd ml-dapr-pipeline

# 全メンバーの依存を一括インストール (dev ツール含む)
make setup-dev
```

`make setup-dev` は `uv sync --all-packages --dev` を実行する。`.venv/` にワークスペース共通の仮想環境が作成され、全メンバーが editable モードでインストールされる。


### 4.2 ローカル環境の起動

```bash
make up
```

起動するコンテナと接続先は以下の通り。

| コンテナ | 役割 | ホストポート | 備考 |
|---------|------|-----------|------|
| redpanda | Kafka 互換ブローカー | 19092 | Dapr Pub/Sub バックエンド |
| valkey | インメモリ KV ストア | 6379 | Dapr State Store バックエンド |
| postgres | RDBMS | 5432 | MLflow メタデータ |
| minio | S3 互換ストレージ | 9000 (API), 9001 (Console) | モデル/データ保存 |
| mlflow | 実験管理 UI | 5001 | |
| prometheus | メトリクス収集 | 9090 | Dapr sidecar メトリクス |
| grafana | 可視化ダッシュボード | 3000 | admin / admin |
| dapr-placement | Dapr Actor 配置 | 50006 | |
| ingestion + sidecar | データ取り込み | 8001 | |
| preprocessing + sidecar | 前処理 | 8002 | |
| inference + sidecar | 推論 | 8003 | |
| alert + sidecar | アラート | 8004 | |
| dashboard + sidecar | リアルタイムUI | 8005 | |
| training + sidecar | 学習ワークフロー | 8006 | Prefect フロー実行 |
| prefect-server | ワークフロー管理UI | 4200 | Prefect ダッシュボード |


### 4.3 起動確認

```bash
# 全サービス + Dapr のヘルスチェック
make health
```

全項目が `[OK]` になれば準備完了。`[NG]` の項目がある場合は、該当コンテナのログを確認する。

```bash
make logs-ingestion         # サービスログ
docker compose logs ingestion-dapr  # Dapr sidecar ログ
```


### 4.4 サンプルデータの投入

```bash
# 正常データ 10 件 + 異常データ 2 件を投入
make seed
```

データの流れは以下の通り。

```
seed_data.py → POST /ingest (ingestion:8001)
    → Dapr Pub/Sub → raw-data topic (Redpanda)
    → preprocessing が subscribe → 正規化 → preprocessed-data topic
    → inference が subscribe → 異常検知 → inference-results topic
                                         → alerts topic (異常時のみ)
    → alert が subscribe → ログ出力
```

### 4.5 リアルタイムモニタリング

```bash
# ターミナルモニター (2秒毎更新)
make watch
```

ターミナルに各センサーの異常スコア・信頼度・アラート統計が表示される。

Grafana ダッシュボード (`http://localhost:3000`) では、Dapr の Pub/Sub スループット、State Store 操作量、サービスレイテンシがグラフで確認できる。


---

## 5. 開発ワークフロー

### 5.1 日常の開発サイクル

```
1. コード編集  →  services/*/src/{svc}/*.py または libs/common/src/common/*.py
2. 動作確認    →  make seed (データ投入) + make watch (結果確認)
3. テスト      →  make test
4. リント      →  make lint-fix
5. コミット    →  git push → CI 自動実行
```

### 5.2 ホットリロード開発

Docker を使わず、特定のサービスだけをローカルで直接起動する方法。コード変更が即座に反映される。

```bash
# インフラだけ Docker で起動
docker compose up -d redpanda valkey postgres minio minio-init mlflow dapr-placement

# ingestion をホットリロードで起動
make run-ingestion
```

`make run-ingestion` は内部で以下を実行する。

```bash
uv run --package ingestion-svc \
  uvicorn ingestion.main:app --host 0.0.0.0 --port 8000 --reload
```

`libs/common` の変更も `--reload` で検知されるため、共有ライブラリの修正もリアルタイムに反映される。ただしこの方法では Dapr sidecar が起動しないため、Dapr API を使う機能のテストには方法 A (Docker Compose) を使用する。


### 5.3 新しい依存の追加

```bash
# 特定サービスに追加
uv add --package preprocessing-svc scipy

# 共有ライブラリに追加
uv add --package ml-pipeline-common tenacity

# 開発ツールに追加 (全メンバー共通)
uv add --dev mypy
```

`uv add` は該当メンバーの `pyproject.toml` を更新し、`uv.lock` を自動再生成する。`uv.lock` は必ずコミットする（全開発者・CI・Docker ビルドで同一の依存を保証するため）。


### 5.4 新しいサービスの追加

```bash
# 1. ディレクトリ作成
mkdir -p services/new-svc/src/new_svc

# 2. pyproject.toml 作成
cat > services/new-svc/pyproject.toml << 'EOF'
[project]
name = "new-svc"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = ["ml-pipeline-common"]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/new_svc"]
EOF

# 3. ルート pyproject.toml の members に追加
# [tool.uv.workspace] の members リストに "services/new-svc" を追加

# 4. 依存を再同期
uv sync --all-packages --dev

# 5. Dockerfile を既存サービスからコピーして編集
cp services/ingestion/Dockerfile services/new-svc/Dockerfile
# package 名と import パスを変更

# 6. docker-compose.yml にサービスとサイドカーを追加
```


---

## 6. Dapr 開発パターン

### 6.1 Pub/Sub (イベント送信)

```python
from common.dapr_helpers import publish

await publish("raw-data", {"sensor_id": "A", "value": 42})
```

### 6.2 Pub/Sub (イベント受信)

```python
@app.get("/dapr/subscribe")
async def subscribe() -> list:
    return [{"pubsubname": "pubsub", "topic": "raw-data", "route": "/handle"}]

@app.post("/handle")
async def handle(request: Request) -> dict:
    envelope = await request.json()
    data = envelope.get("data", envelope)  # CloudEvent の data を取得
    # 処理...
    return {"status": "ok"}  # 200 → ACK, non-200 → リトライ
```

### 6.3 State Store

```python
from common.dapr_helpers import save_state, get_state

await save_state("key", {"value": 42})
result = await get_state("key")  # dict or None
```

### 6.4 Service Invocation

```python
from common.dapr_helpers import invoke_service

result = await invoke_service("inference", "predict", {"features": [0.1, 0.9]})
```

詳細は `docs/dapr-guide.md` を参照。エラーハンドリング、冪等性パターン、デバッグ手法も記載されている。


---

## 7. テスト

### 7.1 テスト実行

```bash
make test              # 全テスト
make test-ingestion    # 特定サービス
make cov               # カバレッジ付き (HTML レポート生成)
```

### 7.2 テストの書き方

```python
# services/ingestion/tests/test_ingest.py
import pytest
from httpx import AsyncClient
from ingestion.main import app


@pytest.fixture
async def client():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac


@pytest.mark.asyncio
async def test_health(client: AsyncClient):
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
```

Dapr API を呼ぶ処理のテストでは、`httpx` の `MockTransport` や `respx` で `localhost:3500` へのリクエストをモックする。


---

## 8. 監視と可視化

### 8.1 Grafana ダッシュボード

`http://localhost:3000` (admin / admin) にアクセスすると、事前構成済みの「ML Pipeline — Dapr Monitoring」ダッシュボードが利用できる。

| パネル | 表示内容 |
|--------|---------|
| Pub/Sub messages published | トピック別のメッセージ送信レート |
| Service invocation latency | サービス呼び出しの p95 レイテンシ |
| Anomaly detection rate | alerts トピックへのパブリッシュレート |
| State store operations | State Store の読み書き操作量 |
| Dapr sidecar CPU | 各サイドカーの CPU 使用率 |


### 8.2 ターミナルモニター

```bash
make watch
```

Grafana を開かなくても、ターミナル上で各センサーの最新推論結果とアラート統計を確認できる。


### 8.3 Kafka トピック確認

```bash
# トピック一覧
docker compose exec redpanda rpk topic list

# メッセージを直接確認
docker compose exec redpanda rpk topic consume raw-data --num 5
docker compose exec redpanda rpk topic consume inference-results --num 5
```


### 8.4 State Store 確認

```bash
docker compose exec valkey valkey-cli
> KEYS ml-pipeline*
> GET "ml-pipeline||inference:last:sensor-A"
```


---

## 9. コマンドリファレンス

| コマンド | 説明 |
|---------|------|
| `make setup-dev` | 全依存インストール (dev 含む) |
| `make lock` | uv.lock 更新 |
| `make up` | ローカル環境一括起動 |
| `make down` | 全停止 |
| `make logs` / `make logs-{svc}` | ログ表示 |
| `make run-{svc}` | ホットリロード起動 |
| `make status` | コンテナ状態確認 |
| `make seed` | サンプルデータ投入 (12件) |
| `make seed-continuous` | 継続的データ投入 (1件/秒) |
| `make health` | 全ヘルスチェック |
| `make watch` | リアルタイムモニター |
| `make demo` | 起動 → シード → モニター (全自動) |
| `make test` / `make test-{svc}` | テスト実行 |
| `make cov` | カバレッジ付きテスト |
| `make lint` / `make lint-fix` | リント / 自動修正 |
| `make build` / `make build-{svc}` | Docker イメージビルド |
| `make create-topics` | Kafka トピック作成 |
| `make clean` | 全ボリューム削除 |
| `make run-training` | 学習ワークフローサービスをホットリロード起動 |
| `make train-trigger` | ワークフローサービス経由で学習パイプラインをトリガー |
| `make train-trigger-s3` | S3 パス指定で再学習トリガー |


---

## 10. トラブルシューティング

### ポートが競合する

```bash
# 使用中のポートを確認
sudo lsof -i :9092
sudo lsof -i :6379

# 競合する場合は docker-compose.yml のホストポートを変更
# 例: "6379:6379" → "16379:6379"
```

### Dapr sidecar がコンポーネントに接続できない

```bash
# sidecar ログで接続エラーを確認
docker compose logs ingestion-dapr 2>&1 | grep -i "error\|fail"

# コンポーネント設定の確認
cat dapr/components/local/pubsub.yaml

# Dapr メタデータで登録済みコンポーネントを確認
curl -s http://localhost:8001/v1.0/metadata | python -m json.tool
```

### uv sync が失敗する

```bash
# ロックファイルを再生成
uv lock --upgrade

# キャッシュをクリア
uv cache clean
uv sync --all-packages --dev
```

### libs/common の変更が反映されない

```bash
# editable install を再実行
uv sync --all-packages --dev --reinstall-package ml-pipeline-common
```

### Docker ビルドが失敗する

```bash
# ビルドコンテキストはリポジトリルート
docker build -f services/ingestion/Dockerfile .   # 正しい
# docker build services/ingestion/                 # 誤り (uv.lock が見つからない)
```
