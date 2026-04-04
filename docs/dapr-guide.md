# Dapr 開発ガイド

本ドキュメントは、Dapr サイドカーパターンによるサービス開発の実践的なガイドです。


---

## 1. 基本概念

Dapr サイドカーは各サービスの横で動く独立プロセスで、以下のビルディングブロックを HTTP/gRPC API として提供します。

```
┌─────────────────────────┐      ┌──────────────┐
│  Your Service (FastAPI)  │ ───▶ │ Dapr Sidecar │ ───▶  Kafka / Valkey / S3 / ...
│  localhost:8000          │ ◀─── │ localhost:3500│
└─────────────────────────┘      └──────────────┘
```

アプリケーションは `localhost:3500` に HTTP リクエストを送るだけです。
実際のインフラ（Kafka, Valkey, S3 等）との接続は Dapr が行います。


---

## 2. Pub/Sub（イベント駆動メッセージング）

### 2.1 メッセージを送信する

```python
from common.dapr_helpers import publish

# topic にメッセージを publish
await publish("raw-data", {"sensor_id": "A", "value": 42})
```

内部では以下の HTTP リクエストが送られます:

```bash
POST http://localhost:3500/v1.0/publish/pubsub/raw-data
Content-Type: application/json
{"sensor_id": "A", "value": 42}
```


### 2.2 メッセージを受信する

FastAPI で Dapr Pub/Sub のサブスクリプションを宣言します。

```python
# 1. サブスクリプションの宣言（Dapr が起動時に GET /dapr/subscribe を呼ぶ）
@app.get("/dapr/subscribe")
async def subscribe() -> list:
    return [
        {
            "pubsubname": "pubsub",       # dapr component の名前
            "topic": "raw-data",          # 購読する topic
            "route": "/handle-raw-data",  # メッセージを受け取るエンドポイント
        },
    ]

# 2. メッセージハンドラ
@app.post("/handle-raw-data")
async def handle_raw_data(request: Request) -> dict:
    envelope = await request.json()
    data = envelope.get("data", envelope)  # Dapr がラップする CloudEvent の data 部分
    
    # ここでビジネスロジックを実行
    sensor = SensorData(**data)
    ...
    
    return {"status": "ok"}  # 200 を返せば ACK
```

重要なポイント:
- Dapr は CloudEvents 形式でメッセージをラップするため、`envelope["data"]` でペイロードを取得する
- ハンドラが 200 を返せば ACK（処理完了）、それ以外はリトライされる
- `pubsubname` は `dapr/components/local/pubsub.yaml` の `metadata.name` と一致させる


### 2.3 ローカルでの動作確認

```bash
# Redpanda の topic を直接確認
docker compose exec redpanda rpk topic consume raw-data --num 5

# curl で直接 publish
curl -X POST http://localhost:8001/v1.0/publish/pubsub/raw-data \
  -H "Content-Type: application/json" \
  -d '{"sensor_id": "test", "value": 99}'
```


---

## 3. State Store（状態管理）

### 3.1 状態の保存と取得

```python
from common.dapr_helpers import save_state, get_state

# 保存
await save_state("sensor:A:latest", {"value": 42, "timestamp": "..."})

# 取得
result = await get_state("sensor:A:latest")
# result: {"value": 42, "timestamp": "..."} or None
```


### 3.2 内部の HTTP API

```bash
# 保存
POST http://localhost:3500/v1.0/state/statestore
[{"key": "sensor:A:latest", "value": {"value": 42}}]

# 取得
GET http://localhost:3500/v1.0/state/statestore/sensor:A:latest

# 削除
DELETE http://localhost:3500/v1.0/state/statestore/sensor:A:latest
```


### 3.3 ローカルでの動作確認

```bash
# Valkey に直接接続して確認
docker compose exec valkey valkey-cli
> KEYS ml-pipeline*
> GET "ml-pipeline||sensor:A:latest"
```


---

## 4. Service Invocation（サービス間呼び出し）

### 4.1 別のサービスを呼び出す

```python
from common.dapr_helpers import invoke_service

# inference サービスの /predict エンドポイントを呼び出す
result = await invoke_service("inference", "predict", {"features": [0.1, 0.9, 0.3]})
```


### 4.2 内部の HTTP API

```bash
POST http://localhost:3500/v1.0/invoke/inference/method/predict
Content-Type: application/json
{"features": [0.1, 0.9, 0.3]}
```

利点:
- サービスディスカバリが不要（Dapr が app-id で解決）
- ロードバランシング自動
- mTLS 自動（Kubernetes 上）
- リトライポリシー設定可能


---

## 5. Bindings（外部システム連携）

### 5.1 S3 からのファイル取得

```bash
# Dapr binding 経由で S3/MinIO のファイルを取得
POST http://localhost:3500/v1.0/bindings/model-store
{
    "operation": "get",
    "metadata": {
        "key": "models/v1/model.onnx"
    }
}
```


### 5.2 S3 へのファイル保存

```bash
POST http://localhost:3500/v1.0/bindings/model-store
{
    "operation": "create",
    "data": "<base64-encoded-data>",
    "metadata": {
        "key": "results/output.json"
    }
}
```


---

## 6. コンポーネント設定の仕組み

### 6.1 ローカルと AWS の切り替え

同じコンポーネント名で、バックエンドだけを差し替えます。

```yaml
# dapr/components/local/pubsub.yaml
metadata:
  name: pubsub           # ← この名前でアプリから参照
spec:
  type: pubsub.kafka
  metadata:
    - name: brokers
      value: "redpanda:9092"    # ← ローカルは Redpanda

# dapr/components/aws/pubsub.yaml
metadata:
  name: pubsub           # ← 同じ名前
spec:
  type: pubsub.kafka
  metadata:
    - name: brokers
      value: "${MSK_ENDPOINT}"  # ← AWS は MSK
    - name: authType
      value: "iam"
```

アプリケーションコードは `pubsub` という名前だけを知っていれば動作します。


### 6.2 新しいコンポーネントの追加手順

1. `dapr/components/local/` に YAML を作成
2. `dapr/components/aws/` に対応する YAML を作成
3. Docker Compose を再起動（`make down && make up`）
4. アプリケーションコードからコンポーネント名で参照


---

## 7. デバッグ技法

### 7.1 Dapr サイドカーのログ確認

```bash
# 特定サービスの Dapr sidecar ログ
docker compose logs -f ingestion-dapr

# エラーだけをフィルタ
docker compose logs ingestion-dapr 2>&1 | grep -i error
```


### 7.2 Dapr メタデータの確認

```bash
# 登録されているコンポーネント一覧
curl http://localhost:8001/v1.0/metadata | python -m json.tool

# Pub/Sub コンポーネントの状態
curl http://localhost:8001/v1.0/metadata | \
  python -c "import sys,json; d=json.load(sys.stdin); print(json.dumps(d.get('components',[]), indent=2))"
```


### 7.3 パイプライン全体の動作確認

```bash
# 1. ヘルスチェック
make health

# 2. サンプルデータ投入
make seed

# 3. リアルタイムモニター
make watch

# 4. Grafana ダッシュボード
open http://localhost:3000  # admin/admin
```


---

## 8. よくあるパターン

### 8.1 エラーハンドリングとリトライ

Pub/Sub ハンドラが non-200 を返すと Dapr はリトライします。
意図的にリトライさせたくない場合は `{"status": "DROP"}` を返します。

```python
@app.post("/handle-data")
async def handle(request: Request):
    try:
        data = await request.json()
        # 処理...
        return {"status": "ok"}
    except ValidationError:
        # バリデーションエラーはリトライしても無意味 → DROP
        return {"status": "DROP"}
    except Exception:
        # その他のエラー → Dapr がリトライ
        raise
```


### 8.2 冪等性の確保

同じメッセージが複数回配信される可能性があるため、冪等性を確保します。

```python
@app.post("/handle-data")
async def handle(request: Request):
    envelope = await request.json()
    event_id = envelope.get("id")  # CloudEvents の一意 ID

    # State Store で処理済みチェック
    existing = await get_state(f"processed:{event_id}")
    if existing is not None:
        return {"status": "ok"}  # 既に処理済み

    # 処理実行...
    await save_state(f"processed:{event_id}", {"processed_at": "..."})
    return {"status": "ok"}
```


### 8.3 複数トピックの購読

```python
@app.get("/dapr/subscribe")
async def subscribe() -> list:
    return [
        {"pubsubname": "pubsub", "topic": "raw-data",          "route": "/handle-raw"},
        {"pubsubname": "pubsub", "topic": "config-update",     "route": "/handle-config"},
        {"pubsubname": "pubsub", "topic": "model-updated",     "route": "/handle-model"},
    ]
```


### 8.4 外部イベント駆動のワークフロートリガー

Training Service は S3/MinIO のファイル配置イベントを Kafka 経由で受信し、Prefect フローを起動する。

```python
# Training Service のサブスクリプション
@app.get("/dapr/subscribe")
async def subscribe() -> list:
    return [
        {
            "pubsubname": "pubsub",
            "topic": "training-data-events",    # MinIO → Redpanda → Dapr
            "route": "/events/training-data",
        },
    ]

@app.post("/events/training-data")
async def handle_training_data_event(request: dict, background: BackgroundTasks) -> dict:
    data = request.get("data", request)
    records = data.get("Records", [])      # S3 イベント通知形式
    for record in records:
        s3_key = record["s3"]["object"]["key"]
        # Prefect フローをバックグラウンドで起動
        background.add_task(run_training_flow, s3_key)
    return {"status": "SUCCESS"}
```

このパターンにより、データファイルの配置をトリガーとした自動学習が実現できる。
ローカルでは MinIO Bucket Notification → Redpanda、本番では S3 → EventBridge → MSK が使われる。
