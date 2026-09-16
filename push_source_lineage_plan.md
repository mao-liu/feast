# Implementation Plan: Upstream FeatureView Lineage for `PushSource`

## 1. Overview & Motivation

In ML workflows, derived features are often calculated by external stream/batch computation pipelines (e.g., Spark, Flink, Kafka Streams, or Airflow jobs) that read features from one or more upstream `FeatureView`s and push the resulting features into a downstream `FeatureView` backed by a `PushSource`.

Currently, Feast only models:
- $\text{PushSource} \longrightarrow \text{Target FeatureView}$

This plan adds support for modeling and visualizing the full end-to-end dependency chain in Feast's lineage graphs:
$$\text{Upstream FeatureViews} \longrightarrow \text{PushSource (External Pipeline)} \longrightarrow \text{Target FeatureView}$$

### Scope Boundaries:
- **`PushSource` only**: Upstream feature view lineage (`source_views` / `upstream_feature_views`) is strictly limited to `PushSource`.
- **`BatchSource` and non-push `StreamSource`**: These data sources do not declare or require upstream feature view lineage.
- **Embedded `PushSource` discovery**: When a `PushSource` is used as the source for a `FeatureView`, Feast stores it internally in `stream_source` (and `lv.spec.source` for `LabelView`). Therefore, lineage discovery inspects `registry.data_sources`, `fv.spec.stream_source`, `sfv.spec.stream_source`, and `lv.spec.source`. `batch_source` is never a `PushSource` and is excluded from push candidate discovery.

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

In `RegistryLineageGenerator._parse_direct_relationships()`, extract upstream dependencies exclusively for `PushSource`s.
Candidate `PushSource`s are discovered from:
1. `registry.data_sources` (standalone registered PushSources)
2. `feature_view.spec.stream_source` / `stream_feature_view.spec.stream_source` (embedded PushSources)
3. `label_view.spec.source` (embedded PushSources in LabelViews)

*Note: `batch_source` is never a `PushSource` and does not declare `upstream_feature_views`.*

```python
# Upstream FeatureView -> DataSource (PushSource) relationships
candidate_sources = list(registry.data_sources)
for fv in registry.feature_views:
    if hasattr(fv, "spec") and fv.spec and hasattr(fv.spec, "stream_source") and fv.spec.stream_source:
        candidate_sources.append(fv.spec.stream_source)
for sfv in registry.stream_feature_views:
    if hasattr(sfv, "spec") and sfv.spec and hasattr(sfv.spec, "stream_source") and sfv.spec.stream_source:
        candidate_sources.append(sfv.spec.stream_source)
for lv in registry.label_views:
    if hasattr(lv, "spec") and lv.spec and hasattr(lv.spec, "source") and lv.spec.source:
        candidate_sources.append(lv.spec.source)

push_sources = [
    ds
    for ds in candidate_sources
    if (
        hasattr(ds, "name")
        and ds.name
        and hasattr(ds, "push_options")
        and ds.push_options
        and hasattr(ds.push_options, "upstream_feature_views")
    )
]

seen_push_edges: Set[Tuple[str, str]] = set()
for ds in push_sources:
    for upstream_fv in ds.push_options.upstream_feature_views:
        edge_key = (upstream_fv, ds.name)
        if edge_key not in seen_push_edges:
            seen_push_edges.add(edge_key)
            source_type = (
                FeastObjectType.LABEL_VIEW
                if upstream_fv in label_view_names
                else FeastObjectType.FEATURE_VIEW
            )
            relationships.append(
                EntityRelation(
                    source=EntityReference(source_type, upstream_fv),
                    target=EntityReference(
                        FeastObjectType.DATA_SOURCE, ds.name
                    ),
                )
            )
```

---

### D. UI Relationship Parser
**File**: `ui/src/parsers/parseEntityRelationships.ts`

In `parseEntityRelationships()`, extract upstream edges exclusively for `PushSource`s from `dataSources`, `fv.spec.streamSource`, `sfv.spec.streamSource`, and `lv.spec.source`:

```typescript
const candidatePushSources = [
  ...((objects as any).dataSources || []),
  ...(objects.featureViews || [])
    .map((fv: any) => fv.spec?.streamSource)
    .filter(Boolean),
  ...(objects.streamFeatureViews || [])
    .map((sfv: any) => sfv.spec?.streamSource)
    .filter(Boolean),
  ...(((objects as any).labelViews || []) as any[])
    .map((lv: any) => lv.spec?.source)
    .filter(Boolean),
];

const seenPushEdges = new Set<string>();
candidatePushSources.forEach((ds: any) => {
  const dsObj = ds.spec || ds;
  const dsName = dsObj.name;
  const pushOpts = dsObj.pushOptions || dsObj.push_options;
  const upstreamFvs =
    pushOpts?.upstreamFeatureViews || pushOpts?.upstream_feature_views;
  if (dsName && Array.isArray(upstreamFvs)) {
    upstreamFvs.forEach((upstreamFvName: string) => {
      const edgeKey = `${upstreamFvName}->${dsName}`;
      if (!seenPushEdges.has(edgeKey)) {
        seenPushEdges.add(edgeKey);
        const isLabelView = labelViewNames.has(upstreamFvName);
        links.push({
          source: {
            type: isLabelView
              ? FEAST_FCO_TYPES["labelView"]
              : FEAST_FCO_TYPES["featureView"],
            name: upstreamFvName,
          },
          target: {
            type: FEAST_FCO_TYPES["dataSource"],
            name: dsName,
          },
        });
      }
    });
  }
});
```
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


---

## 6. Implementation Notes

To avoid mass regeneration of proto files leading to a huge diff, the protos were
generated by pinning generator tools to a version matching the older tools
used when generating the existing files.
```
uv run \
  --with "grpcio-tools==1.62.2" \
  --with "protobuf==4.25.1" \
  --with "mypy-protobuf==3.3.0" \
  python infra/scripts/generate_protos.py
```

A prior commit on master (28bde0128 which introduced ConnectionRef) updated DataSource.proto but did not commit the generated DataSource_pb2.py / .pyi bindings. Compiling DataSource.proto now brought in both upstream_feature_views and ConnectionRef.


### `parseEntityRelationships.ts` & `registry_lineage.py`

Previously, there was an inconsistency between FeatureView and StreamFeatureView:

- StreamFeatureView: Always drew edges for both its streamSource and its batchSource:
    - streamSource -> StreamFeatureView
    - batchSource -> StreamFeatureView
- Standard FeatureView: Only inspected fv.spec.batchSource:
    - If a standard FeatureView used a PushSource, KafkaSource, or KinesisSource, Feast populated both stream_source and batch_source under the hood.
    - However, the lineage generator (both in Python registry_lineage.py and in UI parseEntityRelationships.ts) completely ignored fv.spec.streamSource.
    - Consequently, the stream/push source node was omitted, and only the batch source was connected.

Now, whenever a standard FeatureView has a streamSource defined (PushSource, KafkaSource, KinesisSource, etc.), the relationship `streamSource -> FeatureView` is always drawn.

