# Dapr 開発ガイド

> 対象読者: 本プロジェクトで Dapr を使ったサービス開発を行うエンジニア
> 最終更新: 2026-04

---

## 1. Dapr とは何か

Dapr (Distributed Application Runtime) は、マイクロサービスの「インフラとの接続部分」を代行してくれるサイドカープロセスです。各サービスの横で動き、Kafka・Valkey・S3 などのインフラとの通信を HTTP API として抽象化します。

### 1.1 サイドカーパターンの仕組み

```
┌──────────────────────────────────────────────────────────────┐
│  Docker Compose / Kubernetes Pod                             │
│                                                              │
│  ┌─────────────────────┐      ┌─────────────────────┐       │
│  │  Your Service        │      │  Dapr Sidecar        │       │
│  │  (FastAPI)           │ ───▶ │  (daprd)             │       │
│  │                      │ ◀─── │                      │       │
│  │  localhost:8000      │      │  localhost:3500      │       │
│  └─────────────────────┘      └──────────┬──────────┘       │
│                                           │                  │
└───────────────────────────────────────────┼──────────────────┘
                                            │
                              ┌─────────────┼─────────────┐
                              ▼             ▼             ▼
                          Redpanda       Valkey        MinIO
                          (Kafka)        (Redis)       (S3)
```

**重要な設計原則**: アプリケーションは `localhost:3500` に HTTP リクエストを送るだけです。Kafka のプロトコルや Valkey のコマンド、S3 の署名を知る必要はありません。インフラの詳細は全て Dapr が吸収します。

### 1.2 本プロジェクトで使用するビルディングブロック

| ビルディングブロック | 用途 | コンポーネント名 | ローカル実装 | AWS 実装 |
|-------------------|------|---------------|-----------|---------|
| **Pub/Sub** | イベント駆動メッセージング | `pubsub` | Redpanda (Kafka) | Amazon MSK |
| **State Store** | 状態の保存・取得 | `statestore` | Valkey (Redis) | ElastiCache |
| **Bindings** | 外部システム連携 | `model-store` | MinIO (S3) | Amazon S3 |
| **Service Invocation** | サービス間 RPC | (組み込み) | ─ | ─ |

環境によらず、アプリケーションコードはコンポーネント名（`pubsub`, `statestore` 等）だけを参照します。

### 1.3 ローカル環境でのサイドカー構成

Docker Compose では、各サービスに対応する `*-dapr` コンテナが `network_mode: "service:<サービス名>"` で起動します。これにより、サービスから `localhost:3500` でサイドカーにアクセスできます。

```yaml
# docker-compose.yml から抜粋
ingestion-dapr:
  image: "daprio/daprd:1.14.1"
  command: ["./daprd",
    "--app-id", "ingestion",          # サービスの識別子
    "--app-port", "8000",             # サービスのポート (Dapr → サービス)
    "--dapr-http-port", "3500",       # Dapr HTTP API ポート (サービス → Dapr)
    "--dapr-grpc-port", "50001",
    "--resources-path", "/components", # コンポーネント設定の読み込み先
    "--config", "/config/config.yaml",
    "--placement-host-address", "dapr-placement:50006"]
  volumes:
    - ./dapr/components/local:/components  # ← ローカル用のコンポーネント定義
    - ./dapr:/config
  network_mode: "service:ingestion"        # ← ingestion と同じネットワーク
```

Kubernetes 上では、Pod の annotation (`dapr.io/enabled: "true"`) により Dapr が自動注入されます。


---

## 2. 共有ヘルパー関数 (`common.dapr_helpers`)

`source/libs/common/src/common/dapr_helpers.py` に、Dapr HTTP API のラッパー関数が定義されています。全サービスがこのモジュールを使います。

### 2.1 関数一覧

| 関数 | 用途 | Dapr API |
|------|------|----------|
| `publish(topic, data)` | Pub/Sub にメッセージ送信 | `POST /v1.0/publish/{pubsub}/{topic}` |
| `save_state(key, value)` | State Store に保存 | `POST /v1.0/state/{store}` |
| `get_state(key)` | State Store から取得 | `GET /v1.0/state/{store}/{key}` |
| `invoke_service(app_id, method, data)` | 別サービスを呼び出し | `POST /v1.0/invoke/{app_id}/method/{method}` |

### 2.2 Dapr ポートの解決

サイドカーのポートは環境変数 `DAPR_HTTP_PORT` で決まります（デフォルト: `3500`）。

```python
DAPR_PORT = int(os.environ.get("DAPR_HTTP_PORT", "3500"))

def _dapr_url(path: str) -> str:
    return f"http://localhost:{DAPR_PORT}{path}"
```

Docker Compose と Kubernetes の両方で `DAPR_HTTP_PORT=3500` が設定されているため、コードの変更は不要です。


---

## 3. Pub/Sub（イベント駆動メッセージング）

パイプラインの中核となるパターンです。各サービスはトピックにメッセージを publish し、下流のサービスが subscribe して処理を継続します。

### 3.1 パイプラインのデータフロー

```
Ingestion ──publish──▶ [raw-data] ──subscribe──▶ Preprocessing
                                                      │
                                              publish  │
                                                      ▼
                                             [preprocessed-data]
                                                      │
                                           subscribe  │
                                                      ▼
                                                 Inference
                                                  │      │
                                           publish │      │ publish (異常時のみ)
                                                  ▼      ▼
                                      [inference-results] [alerts]
                                                          │
                                               subscribe  │
                                                          ▼
                                                        Alert
```

**トピック一覧**:

| トピック名 | 発行者 | 購読者 | ペイロード |
|-----------|--------|--------|-----------|
| `raw-data` | Ingestion | Preprocessing | `SensorData` |
| `preprocessed-data` | Preprocessing | Inference | `ProcessedData` |
| `inference-results` | Inference | (Dashboard等) | `InferenceResult` |
| `alerts` | Inference | Alert | `Alert` |
| `model-updates` | Training (register) | Inference | `ModelUpdateEvent` |
| `training-data-events` | MinIO/S3 通知 | Training | S3 Event Notification |


### 3.2 メッセージを送信する (publish)

```python
from common.dapr_helpers import publish

# topic にメッセージを publish
await publish("raw-data", data.model_dump(mode="json"))
```

内部では以下の HTTP リクエストが Dapr サイドカーに送られます:

```
POST http://localhost:3500/v1.0/publish/pubsub/raw-data
Content-Type: application/json

{"sensor_id": "sensor-A", "timestamp": "2026-04-05T12:00:00Z", ...}
```

Dapr が受け取ったメッセージを Redpanda (Kafka) に転送します。アプリケーションは Kafka のプロトコルを一切意識しません。

**実装例** (Ingestion サービス):

```python
# source/services/ingestion/src/ingestion/main.py
@app.post("/ingest")
async def ingest(data: SensorData) -> dict:
    await publish("raw-data", data.model_dump(mode="json"))
    await save_state(
        f"ingestion:last:{data.sensor_id}",
        {"timestamp": data.timestamp.isoformat(), "values": data.values},
    )
    return {"status": "published", "sensor_id": data.sensor_id}
```


### 3.3 メッセージを受信する (subscribe)

Dapr は起動時にサービスの `GET /dapr/subscribe` を呼び出し、どのトピックをどのエンドポイントに配送するかを学習します。

#### Step 1: サブスクリプション宣言

```python
@app.get("/dapr/subscribe")
async def subscribe() -> list:
    return [
        {
            "pubsubname": "pubsub",           # コンポーネント名 (pubsub.yaml の metadata.name)
            "topic": "raw-data",              # 購読するトピック
            "route": "/process",              # メッセージを配送するエンドポイント
        },
    ]
```

#### Step 2: メッセージハンドラ

```python
@app.post("/process")
async def process(request: Request) -> dict:
    # Dapr は CloudEvents 形式でラップして配送する
    envelope = await request.json()
    raw = envelope.get("data", envelope)  # "data" フィールドに元のペイロードがある

    sensor = SensorData(**raw)
    features, names = normalize(sensor.values)

    # 処理結果を次のトピックに publish
    await publish("preprocessed-data", processed.model_dump(mode="json"))

    return {"status": "SUCCESS"}  # 200 を返すと ACK (処理完了)
```

#### レスポンスによる配送制御

| レスポンス | Dapr の動作 |
|-----------|------------|
| `{"status": "SUCCESS"}` または HTTP 200 | **ACK** — メッセージを処理完了としてマーク |
| `{"status": "RETRY"}` または HTTP 5xx | **RETRY** — 一定時間後にリトライ |
| `{"status": "DROP"}` | **DROP** — メッセージを破棄（リトライしない） |

#### CloudEvents エンベロープの構造

Dapr が配送するメッセージは [CloudEvents](https://cloudevents.io/) 仕様に準拠しています:

```json
{
  "id": "unique-event-id",
  "source": "ingestion",
  "type": "com.dapr.event.sent",
  "specversion": "1.0",
  "datacontenttype": "application/json",
  "topic": "raw-data",
  "pubsubname": "pubsub",
  "data": {
    "sensor_id": "sensor-A",
    "timestamp": "2026-04-05T12:00:00Z",
    "values": {"temperature": 25.0, "humidity": 60.0}
  }
}
```

`envelope.get("data", envelope)` というパターンで、CloudEvents でもプレーンな JSON でも対応できます。


### 3.4 複数トピックの購読

1 つのサービスが複数のトピックを購読できます。Inference サービスが実際にこのパターンを使っています:

```python
# source/services/inference/src/inference/main.py
@app.get("/dapr/subscribe")
async def subscribe() -> list:
    return [
        {"pubsubname": "pubsub", "topic": "preprocessed-data", "route": "/predict"},
        {"pubsubname": "pubsub", "topic": "model-updates", "route": "/model-update"},
    ]
```

- `/predict` — 前処理済みデータを受信して推論を実行
- `/model-update` — 新しいモデルが登録された通知を受信してホットスワップ


### 3.5 rawPayload モード

Training サービスは MinIO からの S3 イベント通知を受信するため、CloudEvents ラッピングをスキップする `rawPayload` モードを使っています:

```python
# source/services/training/src/training_service/main.py
@app.get("/dapr/subscribe")
async def subscribe() -> list:
    return [
        {
            "pubsubname": "pubsub",
            "topic": "training-data-events",
            "route": "/events/training-data",
            "metadata": {
                "rawPayload": "true",  # CloudEvents ラッピングをスキップ
            },
        },
    ]
```

MinIO Bucket Notification は独自のイベント形式（S3 Event Notification 互換）を使うため、CloudEvents としてパースすると壊れる可能性があります。`rawPayload: "true"` を設定すると、Dapr はメッセージをそのまま配送します。


### 3.6 ローカルでの動作確認

```bash
# Redpanda のトピック一覧
docker compose exec redpanda rpk topic list

# トピックのメッセージを直接読む
docker compose exec redpanda rpk topic consume raw-data --num 5
docker compose exec redpanda rpk topic consume preprocessed-data --num 3
docker compose exec redpanda rpk topic consume alerts --num 3

# Dapr API 経由で直接 publish (ingestion サイドカーのポートを使用)
curl -X POST http://localhost:3501/v1.0/publish/pubsub/raw-data \
  -H "Content-Type: application/json" \
  -d '{"sensor_id": "test-sensor", "values": {"temperature": 99.0}}'
```

> **ポート 3501** は ingestion サービスの Dapr sidecar HTTP API がホスト側に公開しているポートです (`docker-compose.yml` で `3501:3500` とマッピング)。他のサービスのサイドカーはホスト側にポートを公開していないため、ingestion 経由でテストします。


---

## 4. State Store（状態管理）

各サービスが処理の最新状態を保存・取得するために使います。ローカルでは Valkey (Redis 互換)、AWS では ElastiCache がバックエンドです。

### 4.1 状態の保存

```python
from common.dapr_helpers import save_state

# キーと値のペアを保存
await save_state(
    "inference:last:sensor-A",
    {"prediction": 0.92, "is_anomaly": True, "model_version": "v1.0.0"},
)
```

内部 HTTP API:

```
POST http://localhost:3500/v1.0/state/statestore
Content-Type: application/json

[{"key": "inference:last:sensor-A", "value": {"prediction": 0.92, ...}}]
```

### 4.2 状態の取得

```python
from common.dapr_helpers import get_state

result = await get_state("inference:last:sensor-A")
# result: {"prediction": 0.92, "is_anomaly": True, ...}
# キーが存在しない場合: None
```

内部 HTTP API:

```
GET http://localhost:3500/v1.0/state/statestore/inference:last:sensor-A
```

- HTTP 200 + JSON body → データあり
- HTTP 204 (No Content) → データなし → `get_state()` は `None` を返す


### 4.3 キーの命名規約

本プロジェクトでは `{サービス名}:{用途}:{識別子}` の形式を使っています:

| キーパターン | サービス | 用途 |
|------------|---------|------|
| `ingestion:last:{sensor_id}` | Ingestion | 最後に取り込んだデータ |
| `preprocessing:last:{sensor_id}` | Preprocessing | 最後の前処理結果 |
| `inference:last:{sensor_id}` | Inference | 最新の推論結果 |
| `alert:last:{sensor_id}` | Alert | 最新のアラート |

### 4.4 Valkey のキープレフィックス

`statestore.yaml` で `keyPrefix: "ml-pipeline"` が設定されているため、Dapr が Valkey に保存する実際のキーは `ml-pipeline||{app-id}||{key}` の形式になります。

```bash
# Valkey に直接接続して確認
docker compose exec valkey valkey-cli

> KEYS ml-pipeline*
# 例: "ml-pipeline||ingestion||ingestion:last:sensor-A"

> GET "ml-pipeline||inference||inference:last:sensor-A"
```


---

## 5. Service Invocation（サービス間呼び出し）

Dapr 経由で別のサービスを直接呼び出せます。Pub/Sub が非同期のイベント駆動なのに対し、Service Invocation は同期的な RPC です。

### 5.1 呼び出し方

```python
from common.dapr_helpers import invoke_service

# inference サービスの /predict エンドポイントを呼び出す
result = await invoke_service("inference", "predict", {"features": [0.1, 0.9, 0.3]})
```

内部 HTTP API:

```
POST http://localhost:3500/v1.0/invoke/inference/method/predict
Content-Type: application/json

{"features": [0.1, 0.9, 0.3]}
```

### 5.2 Pub/Sub との使い分け

| | Pub/Sub | Service Invocation |
|---|---------|-------------------|
| 通信パターン | 非同期 (Fire & Forget) | 同期 (Request/Response) |
| 結合度 | 疎結合（発行者は購読者を知らない） | 密結合（呼び出し先を指定する） |
| 戻り値 | なし | あり |
| リトライ | Dapr が自動リトライ | アプリケーション側で制御 |
| 用途 | パイプライン処理、イベント通知 | データ取得、ステータス確認 |

本プロジェクトではパイプラインのデータフロー全体を **Pub/Sub** で実装し、個別のデータ取得 API (`/results/{sensor_id}`, `/model/status` 等) を **Service Invocation** で呼び出す設計にしています。

### 5.3 利点

- **サービスディスカバリ不要** — `app-id` で自動解決（DNS や環境変数の管理が不要）
- **ロードバランシング** — 同じ `app-id` の複数インスタンスに自動分散
- **mTLS** — Kubernetes 上では Dapr が自動的に相互 TLS を適用
- **リトライポリシー** — `dapr/config.yaml` でグローバルに設定可能


---

## 6. Bindings（外部システム連携）

S3/MinIO のファイル操作に使います。コンポーネント名は `model-store` です。

### 6.1 ファイルの取得

```bash
POST http://localhost:3500/v1.0/bindings/model-store
Content-Type: application/json

{
    "operation": "get",
    "metadata": {
        "key": "models/v1/model.onnx"
    }
}
```

### 6.2 ファイルの保存

```bash
POST http://localhost:3500/v1.0/bindings/model-store
Content-Type: application/json

{
    "operation": "create",
    "data": "<base64-encoded-data>",
    "metadata": {
        "key": "results/output.json"
    }
}
```

### 6.3 ローカルの Binding 設定

```yaml
# dapr/components/local/binding-s3.yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: model-store
spec:
  type: bindings.aws.s3
  version: v1
  metadata:
    - name: bucket
      value: "ml-models"
    - name: endpoint
      value: "http://minio:9000"     # ← ローカルは MinIO
    - name: forcePathStyle
      value: "true"                  # MinIO は path-style を必要とする
    - name: accessKey
      value: "minioadmin"
    - name: secretKey
      value: "minioadmin"
```

AWS 環境では `endpoint` が不要になり、認証は IRSA (IAM Roles for Service Accounts) に切り替わります。


---

## 7. コンポーネント設定

### 7.1 ディレクトリ構成

```
dapr/
├── config.yaml                  ← Dapr 全体の設定 (トレーシング、メトリクス)
└── components/
    ├── local/                   ← ローカル開発用
    │   ├── pubsub.yaml          ← Redpanda (Kafka)
    │   ├── statestore.yaml      ← Valkey (Redis)
    │   └── binding-s3.yaml      ← MinIO (S3)
    ├── aws/                     ← AWS 本番用
    │   ├── pubsub.yaml          ← Amazon MSK
    │   ├── statestore.yaml      ← ElastiCache
    │   └── binding-s3.yaml      ← Amazon S3
    └── dashboard/               ← Dashboard 専用 (読み取りのみのコンポーネント)
        ├── pubsub.yaml
        └── statestore.yaml
```

### 7.2 環境の切り替え

Docker Compose ではサイドカーのボリュームマウントで `local/` を使用:

```yaml
volumes:
  - ./dapr/components/local:/components
```

Kubernetes (Helm) では `aws/` の内容が ConfigMap として適用されます。

**アプリケーションコードは一切変更しません。** コンポーネント名 (`pubsub`, `statestore`, `model-store`) が一致していれば、バックエンドが Redpanda でも MSK でも同じように動作します。

### 7.3 ローカルとAWSのコンポーネント比較

#### Pub/Sub

| 設定項目 | ローカル (`local/pubsub.yaml`) | AWS (`aws/pubsub.yaml`) |
|---------|------|------|
| `type` | `pubsub.kafka` | `pubsub.kafka` |
| `brokers` | `redpanda:9092` | `${MSK_BOOTSTRAP_BROKERS}` |
| `authType` | `none` | `iam` |
| `consumerGroup` | (デフォルト) | `ml-pipeline` |

#### State Store

| 設定項目 | ローカル (`local/statestore.yaml`) | AWS (`aws/statestore.yaml`) |
|---------|------|------|
| `type` | `state.redis` | `state.redis` |
| `redisHost` | `valkey:6379` | `${ELASTICACHE_ENDPOINT}` |
| `redisPassword` | (空) | Kubernetes Secret 参照 |
| `enableTLS` | (なし) | `true` |

#### S3 Binding

| 設定項目 | ローカル (`local/binding-s3.yaml`) | AWS (`aws/binding-s3.yaml`) |
|---------|------|------|
| `type` | `bindings.aws.s3` | `bindings.aws.s3` |
| `bucket` | `ml-models` | `${S3_MODEL_BUCKET}` |
| `endpoint` | `http://minio:9000` | (不要 — AWS デフォルト) |
| 認証 | accessKey / secretKey | IRSA (`authProfile: irsa`) |


### 7.4 Dapr 全体設定 (`config.yaml`)

```yaml
# dapr/config.yaml
apiVersion: dapr.io/v1alpha1
kind: Configuration
metadata:
  name: ml-pipeline-config
spec:
  tracing:
    samplingRate: "1"             # 全リクエストをトレース (開発環境用)
    otel:
      endpointAddress: "otel-collector:4317"
      isSecure: false
      protocol: grpc
  metrics:
    enabled: true                 # Prometheus メトリクスを有効化
  accessControl:
    defaultAction: allow          # 全サービス間の通信を許可
```

- `samplingRate: "1"` — 100% サンプリング。本番では `"0.1"` (10%) 等に下げることを検討
- `metrics: enabled: true` — Dapr が Prometheus 形式のメトリクスを自動出力
- `accessControl: defaultAction: allow` — 開発環境では全許可。本番ではサービスごとのポリシーを設定可能

### 7.5 新しいコンポーネントの追加手順

1. `dapr/components/local/` に YAML を作成
2. `dapr/components/aws/` に対応する YAML を作成（コンポーネント名を一致させる）
3. Docker Compose を再起動 (`make down && make up`)
4. アプリケーションコードからコンポーネント名で参照
5. Helm values にも設定を追加（本番デプロイ用）


---

## 8. よくあるパターン

### 8.1 エラーハンドリングとリトライ

Pub/Sub ハンドラでエラーが発生した場合の制御:

```python
@app.post("/process")
async def process(request: Request) -> dict:
    envelope = await request.json()
    raw = envelope.get("data", envelope)

    try:
        sensor = SensorData(**raw)
    except ValidationError:
        # バリデーションエラーはリトライしても無意味 → DROP で破棄
        logger.warning("invalid_data", raw=raw)
        return {"status": "DROP"}

    try:
        # ビジネスロジック
        result = await heavy_processing(sensor)
        await publish("next-topic", result)
        return {"status": "SUCCESS"}
    except httpx.ConnectError:
        # 一時的なエラー → RETRY で Dapr にリトライさせる
        return {"status": "RETRY"}
    except Exception:
        # 予期しないエラー → 例外を上げれば Dapr が 500 を検知してリトライ
        raise
```

**判断基準**:
- **DROP**: データが壊れている、バリデーションエラー → リトライしても意味がない
- **RETRY / 例外**: ネットワーク障害、一時的なリソース不足 → 少し待てば成功する可能性がある
- **SUCCESS**: 正常処理完了


### 8.2 冪等性の確保

Kafka の At-Least-Once 配送保証により、同じメッセージが複数回配送される可能性があります。State Store を使って重複処理を防ぎます。

```python
@app.post("/handle-data")
async def handle(request: Request) -> dict:
    envelope = await request.json()
    event_id = envelope.get("id")  # CloudEvents の一意 ID

    # State Store で処理済みチェック
    existing = await get_state(f"processed:{event_id}")
    if existing is not None:
        return {"status": "SUCCESS"}  # 既に処理済み → スキップ

    # 処理実行
    data = envelope.get("data", envelope)
    await process_data(data)

    # 処理済みフラグを保存
    await save_state(f"processed:{event_id}", {"processed_at": datetime.utcnow().isoformat()})
    return {"status": "SUCCESS"}
```

> **注意**: 本プロジェクトの現行コードでは、パイプラインの各ステップが冪等（同じ入力に対して同じ結果を返す）な設計になっているため、明示的な重複排除は行っていません。State Store への保存は「最新値の上書き」であり、同じデータが再処理されても結果は同じです。重複排除が必要になるのは、外部への通知やカウンターの加算など、副作用のある処理の場合です。


### 8.3 外部イベントによる自動トリガー

Training サービスは、S3/MinIO へのファイル配置をトリガーとして学習パイプラインを自動起動します:

```
[ローカル環境]
MinIO にファイルアップロード (.npy / .csv / .parquet)
  → MinIO Bucket Notification (Kafka プロトコル)
  → Redpanda (training-data-events トピック)
  → Dapr Pub/Sub → Training Service /events/training-data
  → Prefect Flow を BackgroundTasks で起動

[AWS 環境]
S3 PutObject → EventBridge → MSK (training-data-events トピック)
  → Dapr Pub/Sub → Training Service
  → Prefect Flow を起動
```

ハンドラの実装:

```python
# source/services/training/src/training_service/main.py
@app.post("/events/training-data")
async def handle_training_data_event(request: dict, background: BackgroundTasks) -> dict:
    data = request.get("data", request)

    # S3 Event Notification 形式の "Records" 配列をパース
    records = data.get("Records", [])
    for record in records:
        s3_key = record["s3"]["object"]["key"]

        # 学習データファイルかどうかチェック (.npy, .csv, .parquet)
        if not _is_training_data(s3_key):
            continue

        event = TrainingTriggerEvent(
            source=TrainingTriggerEvent.TriggerSource.S3_EVENT,
            s3_bucket=record["s3"]["bucket"]["name"],
            s3_key=s3_key,
        )
        # Prefect フローをバックグラウンドで実行 (API レスポンスをブロックしない)
        background.add_task(_run_flow, event)

    return {"status": "SUCCESS"}
```


### 8.4 モデルのホットスワップ

Inference サービスは `model-updates` トピックを購読し、新しいモデルが MLflow に登録されたら自動的にロードします:

```python
# source/services/inference/src/inference/main.py
@app.post("/model-update")
async def model_update(request: Request) -> dict:
    envelope = await request.json()
    raw = envelope.get("data", envelope)
    event = ModelUpdateEvent(**raw)

    try:
        await model_manager.load_model(
            model_name=event.model_name,
            model_version=event.model_version,
            model_uri=event.model_uri,
            input_dim=event.input_dim,
            anomaly_threshold=event.anomaly_threshold,
        )
        return {"status": "SUCCESS", "model_version": event.model_version}
    except Exception as e:
        # ロード失敗 → RETRY で Dapr にリトライさせる
        return {"status": "RETRY", "message": str(e)}
```

サービスの再起動なしでモデルが切り替わるため、ダウンタイムゼロでモデル更新が可能です。


---

## 9. デバッグ技法

### 9.1 サイドカーのログ確認

```bash
# 特定サービスの Dapr sidecar ログ
docker compose logs -f ingestion-dapr

# エラーだけをフィルタ
docker compose logs ingestion-dapr 2>&1 | grep -i "error\|warn"

# 全サイドカーのログ
docker compose logs -f ingestion-dapr preprocessing-dapr inference-dapr alert-dapr
```

よくあるエラー:
- `component [pubsub] is not initialized` → Redpanda がまだ起動していない。`make health` で確認
- `error connecting to redis` → Valkey が起動していない、またはホスト名が間違っている
- `app channel is not ready` → サービス本体がまだ起動していない (sidecar が先に起動した)


### 9.2 Dapr メタデータの確認

```bash
# 登録されているコンポーネント一覧
curl -s http://localhost:3501/v1.0/metadata | python -m json.tool

# アクティブなサブスクリプション確認
curl -s http://localhost:3501/v1.0/metadata | \
  python -c "
import sys, json
d = json.load(sys.stdin)
for sub in d.get('subscriptions', []):
    print(f\"  {sub['topic']} → {sub['rules'][0]['path'] if sub.get('rules') else 'N/A'}\")
"
```


### 9.3 パイプライン全体のデバッグフロー

問題が発生した場合の確認手順:

```bash
# 1. 全サービスの状態確認
make health

# 2. サンプルデータを投入
make seed

# 3. 各トピックにメッセージが流れているか確認
docker compose exec redpanda rpk topic consume raw-data --num 1
docker compose exec redpanda rpk topic consume preprocessed-data --num 1
docker compose exec redpanda rpk topic consume inference-results --num 1

# 4. State Store に結果が保存されているか確認
docker compose exec valkey valkey-cli KEYS "ml-pipeline*"

# 5. 特定サービスのログを確認
make logs-ingestion
make logs-inference

# 6. Grafana で Dapr メトリクスを確認
open http://localhost:3000  # admin/admin
```

### 9.4 Dapr なしでのテスト

ユニットテストでは Dapr サイドカーを起動せず、HTTP リクエストをモックします:

```python
# httpx の MockTransport を使う例
import httpx
import pytest
from unittest.mock import AsyncMock, patch

@pytest.mark.asyncio
async def test_publish():
    with patch("common.dapr_helpers.httpx.AsyncClient") as mock_client:
        mock_instance = AsyncMock()
        mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
        mock_instance.__aexit__ = AsyncMock(return_value=False)
        mock_instance.post = AsyncMock(
            return_value=httpx.Response(200)
        )
        mock_client.return_value = mock_instance

        from common.dapr_helpers import publish
        await publish("test-topic", {"key": "value"})

        mock_instance.post.assert_called_once()
```

統合テスト (Docker Compose 環境) では、`make seed` でデータを投入し、`make watch` で結果を確認するのが最も簡単です。


---

## 10. 本番環境での注意事項

### 10.1 Kubernetes での Dapr 設定

Kubernetes 上では Docker Compose のような明示的なサイドカーコンテナ定義は不要です。Deployment の Pod template に annotation を追加するだけです:

```yaml
# helm/templates/deployment.yaml (概要)
metadata:
  annotations:
    dapr.io/enabled: "true"
    dapr.io/app-id: "ingestion"
    dapr.io/app-port: "8000"
    dapr.io/log-level: "info"
    dapr.io/config: "ml-pipeline-config"
```

### 10.2 セキュリティ

| 項目 | ローカル | 本番 |
|------|--------|------|
| Pub/Sub 認証 | なし | MSK IAM 認証 |
| State Store 通信 | 平文 | TLS 暗号化 |
| State Store パスワード | なし | Kubernetes Secret |
| S3 認証 | MinIO アクセスキー | IRSA (IAM Roles for Service Accounts) |
| サービス間通信 | 平文 | Dapr mTLS (自動) |
| アクセス制御 | 全許可 | サービスごとのポリシー設定推奨 |

### 10.3 パフォーマンスチューニング

- `samplingRate` を本番では `"0.1"` (10%) 程度に下げる
- Pub/Sub の `maxMessageBytes` はデフォルト 1MB。大きなデータを送る場合は増やす
- State Store への書き込みが多い場合は、バルク操作 (`POST /v1.0/state/statestore` に複数キーを配列で送る) を検討


---

## 11. クイックリファレンス

### Dapr HTTP API

| 操作 | メソッド | URL |
|------|---------|-----|
| Publish | POST | `/v1.0/publish/{pubsub}/{topic}` |
| State 保存 | POST | `/v1.0/state/{store}` |
| State 取得 | GET | `/v1.0/state/{store}/{key}` |
| State 削除 | DELETE | `/v1.0/state/{store}/{key}` |
| Service 呼出 | POST | `/v1.0/invoke/{app-id}/method/{method}` |
| Binding | POST | `/v1.0/bindings/{name}` |
| メタデータ | GET | `/v1.0/metadata` |
| ヘルスチェック | GET | `/v1.0/healthz` |

### コンポーネント名

| 名前 | 用途 | 設定ファイル |
|------|------|------------|
| `pubsub` | メッセージング | `dapr/components/*/pubsub.yaml` |
| `statestore` | 状態管理 | `dapr/components/*/statestore.yaml` |
| `model-store` | S3 ファイル操作 | `dapr/components/*/binding-s3.yaml` |

### 関連 Make コマンド

| コマンド | 説明 |
|---------|------|
| `make up` | 全サービス + サイドカー起動 |
| `make health` | 全サービス + Dapr のヘルスチェック |
| `make seed` | サンプルデータ投入 (パイプライン全体テスト) |
| `make watch` | リアルタイムモニター |
| `make logs-{svc}` | サービスログ確認 |
| `docker compose logs {svc}-dapr` | サイドカーログ確認 |
| `make create-topics` | Kafka トピック手動作成 |
