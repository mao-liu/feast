from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

def generate_sample_data():
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc)
    user_ids = [1001, 1002, 1003, 1004, 1005]

    # 1. Transactions
    df_tx = pd.DataFrame({
        "user_id": user_ids,
        "tx_amount": [120.5, 45.0, 990.25, 12.0, 310.8],
        "event_timestamp": [now] * len(user_ids),
        "created": [now] * len(user_ids),
    })
    df_tx.to_parquet(data_dir / "transactions.parquet")

    # 2. Credit Profile
    df_credit = pd.DataFrame({
        "user_id": user_ids,
        "credit_score": [720, 650, 810, 590, 750],
        "event_timestamp": [now] * len(user_ids),
        "created": [now] * len(user_ids),
    })
    df_credit.to_parquet(data_dir / "credit_profile.parquet")

    # 3. Daily Summary
    df_summary = pd.DataFrame({
        "user_id": user_ids,
        "total_spend_today": [150.0, 50.0, 1000.0, 20.0, 400.0],
        "event_timestamp": [now] * len(user_ids),
        "created": [now] * len(user_ids),
    })
    df_summary.to_parquet(data_dir / "daily_summary.parquet")

    # 4. Raw Push Offline Backing
    df_raw_push = pd.DataFrame({
        "user_id": user_ids,
        "push_val": [1.1, 2.2, 3.3, 4.4, 5.5],
        "event_timestamp": [now] * len(user_ids),
        "created": [now] * len(user_ids),
    })
    df_raw_push.to_parquet(data_dir / "raw_push.parquet")

    # 5. Risk Offline Backing
    df_risk = pd.DataFrame({
        "user_id": user_ids,
        "composite_risk_score": [0.15, 0.45, 0.05, 0.85, 0.22],
        "risk_tier": [1, 2, 1, 4, 2],
        "event_timestamp": [now] * len(user_ids),
        "created": [now] * len(user_ids),
    })
    df_risk.to_parquet(data_dir / "risk_offline.parquet")

    # 6. Stream Push Offline Backing
    df_stream = pd.DataFrame({
        "user_id": user_ids,
        "stream_click_count": [12, 4, 33, 1, 18],
        "event_timestamp": [now] * len(user_ids),
        "created": [now] * len(user_ids),
    })
    df_stream.to_parquet(data_dir / "stream_push.parquet")

    # 7. Kafka Batch Backing
    df_kafka = pd.DataFrame({
        "user_id": user_ids,
        "clicks": [5, 10, 15, 20, 25],
        "event_timestamp": [now] * len(user_ids),
        "created": [now] * len(user_ids),
    })
    df_kafka.to_parquet(data_dir / "kafka_batch.parquet")

    # 8. Kinesis Batch Backing
    df_kinesis = pd.DataFrame({
        "user_id": user_ids,
        "event_count": [100, 200, 300, 400, 500],
        "event_timestamp": [now] * len(user_ids),
        "created": [now] * len(user_ids),
    })
    df_kinesis.to_parquet(data_dir / "kinesis_batch.parquet")

    print(f"Generated sample datasets in {data_dir.resolve()}")

if __name__ == "__main__":
    generate_sample_data()
