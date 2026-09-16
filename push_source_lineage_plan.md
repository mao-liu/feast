# Implementation Plan: Upstream FeatureView Lineage for `PushSource`

## 1. Overview & Motivation

In ML workflows, derived features are often calculated by external stream/batch computation pipelines (e.g., Spark, Flink, Kafka Streams, or Airflow jobs) that read features from one or more upstream `FeatureView`s and push the resulting features into a downstream `FeatureView` backed by a `PushSource`.

Currently, Feast only models:
- $\text{PushSource} \longrightarrow \text{Target FeatureView}$

This plan adds support for modeling and visualizing the full end-to-end dependency chain in Feast's lineage graphs:
$$\text{Upstream FeatureViews} \longrightarrow \text{PushSource (External Pipeline)} \longrightarrow \text{Target FeatureView}$$

---

## 2. Target Lineage Graph

```
┌─────────────────────────┐
│ user_transaction_stats  │ ──┐
│     (FeatureView)       │   │
└─────────────────────────┘   │     ┌───────────────────────┐       ┌────────────────────────┐
                              ├───→ │   risk_calc_pipeline  │ ────→ │   user_risk_target_fv  │
┌─────────────────────────┐   │     │      (PushSource)     │       │      (FeatureView)     │
│   user_credit_profile   │ ──┘     └───────────────────────┘       └────────────────────────┘
│     (FeatureView)       │
└─────────────────────────┘
```

---

## 3. Required Changes by Component

### A. Protobuf Schema Updates
**File**: `protos/feast/core/DataSource.proto`

Update `PushOptions` to declare upstream feature view references:

```protobuf
message DataSource {
  ...
  message PushOptions {
    reserved 1;
    // Names of upstream FeatureViews consumed by the process feeding this PushSource
    repeated string upstream_feature_views = 2;
  }
}
```

Recompile protobuf bindings:
```bash
make compile-protos-python
make protos
```

---

### B. Python SDK Updates
**File**: `sdk/python/feast/data_source.py`

1. Update `PushSource.__init__` to accept `source_views`:
   ```python
   def __init__(
       self,
       *,
       name: str,
       batch_source: Optional[DataSource] = None,
       source_views: Optional[List[Union["BaseFeatureView", str]]] = None,
       description: Optional[str] = "",
       tags: Optional[Dict[str, str]] = None,
       owner: Optional[str] = "",
   ):
       super().__init__(name=name, description=description, tags=tags, owner=owner)
       self.batch_source = batch_source
       self.source_views: List[str] = [
           sv.name if hasattr(sv, "name") else sv for sv in (source_views or [])
       ]
   ```

2. Update `to_proto()`:
   ```python
   def to_proto(self) -> DataSourceProto:
       data_source_proto = super().to_proto()
       data_source_proto.type = DataSourceProto.SourceType.PUSH_SOURCE
       data_source_proto.push_options.upstream_feature_views.extend(self.source_views)
       return data_source_proto
   ```

3. Update `from_proto()`:
   ```python
   @staticmethod
   def from_proto(data_source: DataSourceProto):
       batch_source = (
           DataSource.from_proto(data_source.batch_source)
           if data_source.HasField("batch_source")
           else None
       )
       return PushSource(
           name=data_source.name,
           batch_source=batch_source,
           source_views=list(data_source.push_options.upstream_feature_views),
           description=data_source.description,
           tags=dict(data_source.tags),
           owner=data_source.owner,
       )
   ```

4. Update `__eq__` to compare `self.source_views == other.source_views`.

---

### C. Static Registry Lineage Engine
**File**: `sdk/python/feast/lineage/registry_lineage.py`

In `RegistryLineageGenerator._parse_direct_relationships()`, extract upstream dependencies for `PushSource`s:

```python
# Upstream FeatureView -> DataSource (PushSource) relationships
for data_source in registry.data_sources:
    if (
        hasattr(data_source, "push_options")
        and data_source.push_options
        and hasattr(data_source.push_options, "upstream_feature_views")
    ):
        for upstream_fv in data_source.push_options.upstream_feature_views:
            relationships.append(
                EntityRelation(
                    source=EntityReference(
                        FeastObjectType.FEATURE_VIEW, upstream_fv
                    ),
                    target=EntityReference(
                        FeastObjectType.DATA_SOURCE, data_source.name
                    ),
                )
            )
```

---

### D. UI Relationship Parser
**File**: `ui/src/parsers/parseEntityRelationships.ts`

In `parseEntityRelationships()`, extract upstream edges for data sources:

```typescript
objects.dataSources?.forEach((ds) => {
  if (ds.pushOptions?.upstreamFeatureViews) {
    ds.pushOptions.upstreamFeatureViews.forEach((upstreamFvName: string) => {
      links.push({
        source: {
          type: FEAST_FCO_TYPES["featureView"],
          name: upstreamFvName,
        },
        target: {
          type: FEAST_FCO_TYPES["dataSource"],
          name: ds.name || "",
        },
      });
    });
  }
});
```

---

### E. OpenLineage Integration
**Files**: `sdk/python/feast/openlineage/mappers.py`, `sdk/python/feast/openlineage/emitter.py`

When emitting lineage events during `feast apply`:
- For `PushSource` definitions with `source_views`, emit OpenLineage job definitions where:
  - **Inputs**: Upstream `FeatureView` dataset nodes (`push_source.source_views`)
  - **Outputs**: `PushSource` dataset node (which then feeds into target `FeatureView`)

---

## 4. User Code Example

```python
from datetime import timedelta
from feast import FeatureView, Field, FileSource, PushSource
from feast.types import Float32, Int64

# 1. Upstream feature views
user_tx_fv = FeatureView(
    name="user_transaction_stats",
    ...,
)
user_credit_fv = FeatureView(
    name="user_credit_profile",
    ...,
)

# 2. PushSource linking offline storage + upstream feature views
risk_push_source = PushSource(
    name="risk_calc_pipeline",
    batch_source=FileSource(path="data/risk_offline.parquet"),
    source_views=[user_tx_fv, user_credit_fv],  # <-- Upstream dependencies
    description="Spark streaming pipeline computing real-time risk scores",
)

# 3. Target feature view receiving the pushed values
user_risk_fv = FeatureView(
    name="user_risk_target_fv",
    entities=[user_entity],
    ttl=timedelta(days=30),
    source=risk_push_source,
    schema=[
        Field(name="composite_risk_score", dtype=Float32),
        Field(name="risk_tier", dtype=Int64),
    ],
)
```

---

## 5. Testing & Validation Checklist

1. **Python Unit Tests**:
   - `sdk/python/tests/unit/test_data_source.py`: Validate protobuf serialization/deserialization with `source_views` passed as both `FeatureView` objects and strings.
   - `sdk/python/tests/unit/infra/test_registry_lineage.py`: Verify that `RegistryLineageGenerator` generates `FeatureView ➔ DataSource` relationships for `PushSource`.
2. **UI Unit Tests**:
   - `ui/src/components/RegistryVisualization.test.tsx`: Test that `parseEntityRelationships` parses `upstreamFeatureViews` on data sources into valid React Flow edges.
3. **Integration Tests**:
   - `sdk/python/tests/integration/rest_api/test_registry_rest_api.py`: Verify `/lineage/registry` and `/lineage/objects/featureView/{name}` endpoints include the new relationships.
4. **Code Quality Checks**:
   ```bash
   uv run ruff check --fix sdk/python/feast/
   uv run ruff format sdk/python/feast/
   uv run bash -c "cd sdk/python && mypy feast"
   cd ui && yarn format:check
   ```
