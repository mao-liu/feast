from datetime import timedelta
import pandas as pd

from feast import (
    BatchFeatureView,
    Entity,
    FeatureService,
    FeatureView,
    Field,
    FileSource,
    KafkaSource,
    KinesisSource,
    PushSource,
    StreamFeatureView,
)
from feast.data_format import JsonFormat
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Int64
from feast.value_type import ValueType

# ---------------------------------------------------------------------------
# 1. Entity
# ---------------------------------------------------------------------------
user = Entity(
    name="user_id",
    join_keys=["user_id"],
    value_type=ValueType.INT64,
    description="Unique identifier for users",
)

# ---------------------------------------------------------------------------
# 2. Batch Data Sources (FileSource)
# ---------------------------------------------------------------------------
user_tx_batch_source = FileSource(
    name="user_tx_batch_source",
    path="data/transactions.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

user_credit_batch_source = FileSource(
    name="user_credit_batch_source",
    path="data/credit_profile.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# user_daily_summary_batch_source = FileSource(
#     name="user_daily_summary_batch_source",
#     path="data/daily_summary.parquet",
#     timestamp_field="event_timestamp",
#     created_timestamp_column="created",
# )

user_raw_push_batch_source = FileSource(
    name="user_raw_push_batch_source",
    path="data/raw_push.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

risk_calc_batch_source = FileSource(
    name="risk_calc_batch_source",
    path="data/risk_offline.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

user_stream_batch_source = FileSource(
    name="user_stream_batch_source",
    path="data/stream_push.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# kafka_batch_source = FileSource(
#     name="kafka_batch_source",
#     path="data/kafka_batch.parquet",
#     timestamp_field="event_timestamp",
#     created_timestamp_column="created",
# )

# kinesis_batch_source = FileSource(
#     name="kinesis_batch_source",
#     path="data/kinesis_batch.parquet",
#     timestamp_field="event_timestamp",
#     created_timestamp_column="created",
# )

# ---------------------------------------------------------------------------
# 3. Upstream Feature Views
# ---------------------------------------------------------------------------
user_transaction_stats = FeatureView(
    name="user_transaction_stats",
    entities=[user],
    ttl=timedelta(days=30),
    schema=[Field(name="tx_amount", dtype=Float32)],
    source=user_tx_batch_source,
    description="User transaction statistics",
)

user_credit_profile = FeatureView(
    name="user_credit_profile",
    entities=[user],
    ttl=timedelta(days=30),
    schema=[Field(name="credit_score", dtype=Int64)],
    source=user_credit_batch_source,
    description="User credit profile history",
)

# ---------------------------------------------------------------------------
# 4. Push & Stream Data Sources
# ---------------------------------------------------------------------------
# PushSource with upstream FeatureView dependencies
risk_calc_pipeline = PushSource(
    name="risk_calc_pipeline",
    batch_source=risk_calc_batch_source,
    source_views=[user_transaction_stats, user_credit_profile],
    description="Spark/Flink streaming pipeline reading tx stats and credit profile",
)

# PushSource without upstream FeatureView dependencies
raw_push_source = PushSource(
    name="raw_push_source",
    batch_source=user_raw_push_batch_source,
    description="Direct push source without upstream views",
)

# PushSource feeding a StreamFeatureView
stream_push_source = PushSource(
    name="stream_push_source",
    batch_source=user_stream_batch_source,
    description="Push source feeding StreamFeatureView",
)

# # Kafka streaming source
# kafka_clickstream_source = KafkaSource(
#     name="kafka_clickstream_source",
#     kafka_bootstrap_servers="localhost:9092",
#     topic="user_clicks",
#     timestamp_field="event_timestamp",
#     message_format=JsonFormat(schema_json=""),
#     batch_source=kafka_batch_source,
#     description="User clickstream from Kafka",
# )

# # Kinesis streaming source
# kinesis_events_source = KinesisSource(
#     name="kinesis_events_source",
#     record_format=JsonFormat(schema_json=""),
#     region="us-east-1",
#     stream_name="user-events-stream",
#     timestamp_field="event_timestamp",
#     batch_source=kinesis_batch_source,
#     description="User event stream from Kinesis",
# )

# ---------------------------------------------------------------------------
# 5. Downstream & Target Feature Views
# ---------------------------------------------------------------------------
# Target FeatureView fed by PushSource with upstream lineage
user_risk_target_fv = FeatureView(
    name="user_risk_target_fv",
    entities=[user],
    ttl=timedelta(days=30),
    schema=[
        Field(name="composite_risk_score", dtype=Float32),
        Field(name="risk_tier", dtype=Int64),
    ],
    source=risk_calc_pipeline,
    description="User risk scores computed by external pipeline and pushed",
)

# FeatureView fed by PushSource without upstream dependencies
user_raw_push_fv = FeatureView(
    name="user_raw_push_fv",
    entities=[user],
    ttl=timedelta(days=30),
    schema=[Field(name="push_val", dtype=Float32)],
    source=raw_push_source,
    description="Raw push feature view",
)

# # FeatureView fed by KafkaSource
# kafka_clicks_fv = FeatureView(
#     name="kafka_clicks_fv",
#     entities=[user],
#     ttl=timedelta(days=30),
#     schema=[Field(name="clicks", dtype=Int64)],
#     source=kafka_clickstream_source,
#     description="Clicks feature view from Kafka stream",
# )

# # FeatureView fed by KinesisSource
# kinesis_events_fv = FeatureView(
#     name="kinesis_events_fv",
#     entities=[user],
#     ttl=timedelta(days=30),
#     schema=[Field(name="event_count", dtype=Int64)],
#     source=kinesis_events_source,
#     description="Events feature view from Kinesis stream",
# )

# # BatchFeatureView
# user_daily_summary_batch_fv = BatchFeatureView(
#     name="user_daily_summary_batch_fv",
#     entities=[user],
#     ttl=timedelta(days=365),
#     schema=[Field(name="total_spend_today", dtype=Float32)],
#     source=user_daily_summary_batch_source,
#     udf=lambda df: df,
#     description="Daily summary computed via batch processing",
# )

# # StreamFeatureView (with PushSource)
# user_stream_fv = StreamFeatureView(
#     name="user_stream_fv",
#     entities=[user],
#     ttl=timedelta(days=1),
#     schema=[Field(name="stream_click_count", dtype=Int64)],
#     source=stream_push_source,
#     udf=lambda df: df,
#     description="Stream feature view with push source",
# )

# # ---------------------------------------------------------------------------
# # 6. On-Demand Feature View
# # ---------------------------------------------------------------------------
# @on_demand_feature_view(
#     sources=[user_transaction_stats, user_risk_target_fv],
#     schema=[Field(name="risk_adjusted_tx_ratio", dtype=Float32)],
#     description="On-demand calculation combining tx stats and risk scores",
# )
# def user_risk_adjusted_feature(inputs: pd.DataFrame) -> pd.DataFrame:
#     df = pd.DataFrame()
#     df["risk_adjusted_tx_ratio"] = (
#         inputs["tx_amount"] * inputs["composite_risk_score"]
#     ).astype("float32")
#     return df

# # ---------------------------------------------------------------------------
# # 7. Feature Service
# # ---------------------------------------------------------------------------
# user_risk_scoring_service = FeatureService(
#     name="user_risk_scoring_service",
#     features=[
#         user_risk_target_fv,
#         user_risk_adjusted_feature,
#         user_daily_summary_batch_fv,
#     ],
#     description="Feature service for user risk scoring inference",
# )
