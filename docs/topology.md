graph LR
    model_store>"model-store<br/>(bindings.aws.s3)"]
    pubsub{{"pubsub<br/>(pubsub.kafka)"}}
    statestore[("statestore<br/>(state.redis)")]
    alert["alert"]
    dashboard["dashboard"]
    inference["inference"]
    ingestion["ingestion"]
    preprocessing["preprocessing"]
    training["training<br/>(Prefect)"]

    alert -->|"use"| statestore
    inference -->|"publish: inference-results"| pubsub
    inference -->|"publish: alerts"| pubsub
    inference -->|"use"| statestore
    ingestion -->|"publish: raw-data"| pubsub
    ingestion -->|"use"| statestore
    preprocessing -->|"publish: preprocessed-data"| pubsub
    preprocessing -->|"use"| statestore
    training -->|"publish: model-updates"| pubsub
    pubsub -->|"subscribe: alerts"| alert
    pubsub -->|"subscribe: alerts"| dashboard
    pubsub -->|"subscribe: inference-results"| dashboard
    pubsub -->|"subscribe: raw-data"| dashboard
    pubsub -->|"subscribe: model-updates"| inference
    pubsub -->|"subscribe: preprocessed-data"| inference
    pubsub -->|"subscribe: raw-data"| preprocessing
    pubsub -->|"subscribe: training-data-events"| training
