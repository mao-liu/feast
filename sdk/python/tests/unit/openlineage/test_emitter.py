# Copyright 2026 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import timedelta
from unittest.mock import MagicMock

import pytest

ol = pytest.importorskip(
    "openlineage.client", reason="openlineage-python not installed"
)

from feast.data_source import PushSource  # noqa: E402
from feast.entity import Entity  # noqa: E402
from feast.feature_view import FeatureView  # noqa: E402
from feast.field import Field  # noqa: E402
from feast.infra.offline_stores.file_source import FileSource  # noqa: E402
from feast.openlineage.config import OpenLineageConfig  # noqa: E402
from feast.openlineage.emitter import FeastOpenLineageEmitter  # noqa: E402
from feast.types import Float32, Int64  # noqa: E402
from feast.value_type import ValueType  # noqa: E402


def test_emit_push_source_lineage():
    """Test emit_push_source_lineage emits event connecting upstream FeatureViews to PushSource."""
    mock_client = MagicMock()
    mock_client.is_enabled = True
    mock_client.emit_run_event.return_value = True

    config = OpenLineageConfig(enabled=True)
    emitter = FeastOpenLineageEmitter(config=config, client=mock_client)

    user_entity = Entity(
        name="user_id", join_keys=["user_id"], value_type=ValueType.INT64
    )
    file_source = FileSource(path="data/transactions.parquet")

    user_tx_fv = FeatureView(
        name="user_transaction_stats",
        entities=[user_entity],
        ttl=timedelta(days=1),
        schema=[Field(name="tx_amount", dtype=Float32)],
        source=file_source,
    )
    user_credit_fv = FeatureView(
        name="user_credit_profile",
        entities=[user_entity],
        ttl=timedelta(days=1),
        schema=[Field(name="credit_score", dtype=Int64)],
        source=file_source,
    )

    push_source = PushSource(
        name="risk_calc_pipeline",
        batch_source=file_source,
        source_views=[user_tx_fv, user_credit_fv],
        description="Streaming pipeline computing risk",
    )

    result = emitter.emit_push_source_lineage(
        push_source=push_source,
        all_feature_views=[user_tx_fv, user_credit_fv],
        project="test_project",
    )

    assert result is True
    assert mock_client.emit_run_event.called

    call_kwargs = mock_client.emit_run_event.call_args.kwargs
    assert call_kwargs["job_name"] == "push_source_risk_calc_pipeline"
    assert call_kwargs["namespace"] == "test_project"

    input_names = [inp.name for inp in call_kwargs["inputs"]]
    assert "user_transaction_stats" in input_names
    assert "user_credit_profile" in input_names

    output_names = [out.name for out in call_kwargs["outputs"]]
    assert "risk_calc_pipeline" in output_names


def test_emit_apply_with_push_source():
    """Test emit_apply emits push source definition job when push source has source_views."""
    mock_client = MagicMock()
    mock_client.is_enabled = True
    mock_client.emit_run_event.return_value = True

    config = OpenLineageConfig(enabled=True, emit_on_apply=True)
    emitter = FeastOpenLineageEmitter(config=config, client=mock_client)

    user_entity = Entity(
        name="user_id", join_keys=["user_id"], value_type=ValueType.INT64
    )
    file_source = FileSource(path="data/transactions.parquet")

    user_tx_fv = FeatureView(
        name="user_transaction_stats",
        entities=[user_entity],
        ttl=timedelta(days=1),
        schema=[Field(name="tx_amount", dtype=Float32)],
        source=file_source,
    )

    push_source = PushSource(
        name="risk_calc_pipeline",
        batch_source=file_source,
        source_views=[user_tx_fv],
    )

    target_fv = FeatureView(
        name="user_risk_target_fv",
        entities=[user_entity],
        ttl=timedelta(days=1),
        schema=[Field(name="risk_score", dtype=Float32)],
        source=push_source,
    )

    results = emitter.emit_apply(
        objects=[user_entity, user_tx_fv, target_fv, push_source],
        project="test_project",
    )

    assert all(results)
    emitted_jobs = [
        call.kwargs["job_name"] for call in mock_client.emit_run_event.call_args_list
    ]
    assert "push_source_risk_calc_pipeline" in emitted_jobs
    assert "feast_feature_views_test_project" in emitted_jobs
