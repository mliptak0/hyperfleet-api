# Pending Reconciliation Metrics Design

## Objective

Capture observability metrics for resources with `Reconciled=False` (pending reconciliation), including:
- Total count of resources entering `Reconciled=False` state (cumulative)
- Current count of resources in `Reconciled=False` state (real-time pending)
- Current count of resources stuck beyond a configurable threshold
- Duration of stuck resources (max time in stuck state)

## Current State

- `ResourceCondition` has `last_transition_time` — exact timestamp when condition status last changed
- Adapter status updates flow through `ProcessAdapterStatus()` in service layer
- Conditions are already aggregated and persisted with transition times
- Deletion observability already uses collector-based approach (see `pkg/metrics/deletion.go`)

## Implementation Plan

### 1. Add Metrics in `pkg/metrics/reconciliation.go` (new file)

Four metrics (following deletion observability pattern):

**Total (Counter)** — Incremented when `Reconciled` transitions True→False:

```go
var pendingReconciliationTotal = prometheus.NewCounterVec(
    prometheus.CounterOpts{
        Subsystem: "hyperfleet_api",
        Name:      "resource_pending_reconciliation_total",
        Help:      "Total number of resources that entered Reconciled=False state",
    },
    []string{"resource_type"},
)

func RecordPendingReconciliation(resourceType string) {
    pendingReconciliationTotal.With(...).Inc()
}
```

**Pending (Collector-based)** — Current count of resources in `Reconciled=False` state:

```go
var pendingDesc *prometheus.Desc = prometheus.NewDesc(
    "hyperfleet_api_resource_pending_reconciliation",
    "Number of resources currently in Reconciled=False state.",
    []string{"resource_type"},
    prometheus.Labels{...},
)
```

**Stuck (Collector-based)** — Count and duration of resources stuck beyond threshold:

```go
type PendingReconciliationCollector struct {
    db               *sql.DB
    queryTimeout     time.Duration
    stuckThreshold   time.Duration
    pendingDesc      *prometheus.Desc
    stuckCountDesc   *prometheus.Desc
    stuckDurationDesc *prometheus.Desc
}

func NewPendingReconciliationCollector(db *sql.DB, stuckThreshold time.Duration) *PendingReconciliationCollector {
    return &PendingReconciliationCollector{
        db:             db,
        stuckThreshold: stuckThreshold,
        queryTimeout:   defaultQueryTimeout,
        pendingDesc: prometheus.NewDesc(
            "hyperfleet_api_resource_pending_reconciliation",
            "Number of resources currently in Reconciled=False state.",
            []string{"resource_type"},
            prometheus.Labels{...},
        ),
        stuckCountDesc: prometheus.NewDesc(
            "hyperfleet_api_resource_pending_reconciliation_stuck",
            "Number of resources in Reconciled=False state beyond stuck threshold.",
            []string{"resource_type"},
            prometheus.Labels{...},
        ),
        stuckDurationDesc: prometheus.NewDesc(
            "hyperfleet_api_resource_pending_reconciliation_stuck_duration_seconds",
            "Maximum duration a resource has been in Reconciled=False state.",
            []string{"resource_type"},
            prometheus.Labels{...},
        ),
    }
}

func (c *PendingReconciliationCollector) Collect(ch chan<- prometheus.Metric) {
    // Query total pending + stuck count + max duration for each resource type
}
```

### 2. Call Observation in Service Layer

**In `pkg/services/cluster.go` and `pkg/services/node_pool.go`**

After aggregation updates conditions, record transition to `Reconciled=False`:

```go
func (s *sqlClusterService) ProcessAdapterStatus(...) (*api.AdapterStatus, *errors.ServiceError) {
    // ... existing aggregation code ...

    oldReconciled := findCondition(cluster.StatusConditions, api.ResourceConditionTypeReconciled)
    newReconciled := findCondition(updatedConditions, api.ResourceConditionTypeReconciled)

    // Transition True → False: record entry into pending reconciliation
    if (oldReconciled == nil || oldReconciled.Status == ConditionTrue) &&
       newReconciled != nil && newReconciled.Status == ConditionFalse {
        metrics.RecordPendingReconciliation(api.ResourceTypeCluster)
    }
}
```

### 3. Configuration

Add to `pkg/config/metrics.go`:

```go
type MetricsConfig struct {
    // ... existing ...
    ReconciliationStuckThreshold time.Duration `mapstructure:"reconciliation_stuck_threshold" default:"10m"`
}
```

Add flag in `pkg/config/loader.go`:

```go
l.bindPFlag("metrics.reconciliation_stuck_threshold", ...)
```

### 4. Registration

Register in `cmd/hyperfleet-api/servecmd/cmd.go`:

```go
metrics.RegisterReconciliationCollector(db, cfg.Metrics.ReconciliationStuckThreshold)
```

## SQL Queries

**Total pending count:**

```sql
SELECT COUNT(*) as total_pending
FROM clusters
WHERE jsonb_path_query_first(status_conditions, '$[*] ? (@.type == "Reconciled")') ->> 'status' = 'False'
```

**Count stuck + max duration:**

```sql
SELECT 
  COUNT(*) as stuck_count,
  MAX(EXTRACT(EPOCH FROM (NOW() - 
      (jsonb_path_query_first(status_conditions, '$[*] ? (@.type == "Reconciled")') ->> 'last_transition_time')::TIMESTAMPTZ
  ))) as max_duration_seconds
FROM clusters
WHERE jsonb_path_query_first(status_conditions, '$[*] ? (@.type == "Reconciled")') ->> 'status' = 'False'
  AND (jsonb_path_query_first(status_conditions, '$[*] ? (@.type == "Reconciled")') ->> 'last_transition_time')::TIMESTAMPTZ < NOW() - interval '10 minutes'
```

Same for `node_pools` table.

## Advantages

- **Consistent pattern**: Mirrors deletion observability (counter + collector)
- **Real-time visibility**: Three-level insight (total pending, currently stuck, worst-case duration)
- **Single collector query**: All three metrics computed in one scrape
- **No extra instrumentation**: Only record event when True→False; rest is automatic DB polling
- **Configurable threshold**: Separate stuck threshold for reconciliation vs deletion
- **Operational clarity**: `pending` shows current load, `stuck` shows problem resources, `duration` shows severity

## Alerting Rules

Add to `charts/templates/prometheusrule.yaml`:

```yaml
- alert: HyperFleetResourceReconciliationStuckWarning
  expr: max by (namespace, resource_type)(hyperfleet_api_resource_pending_reconciliation_stuck) > 0
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "HyperFleet resources stuck in Reconciled=False state"
    description: "{{ $value }} {{ $labels.resource_type }} resource(s) stuck in Reconciled=False for more than {{ .Values.config.metrics.reconciliation_stuck_threshold | default \"10m\" }}"

- alert: HyperFleetResourceReconciliationStuckCritical
  expr: max by (namespace, resource_type)(hyperfleet_api_resource_pending_reconciliation_stuck) > 0
  for: 30m
  labels:
    severity: critical
  annotations:
    summary: "HyperFleet resources critically stuck in Reconciled=False state"
    description: "{{ $value }} {{ $labels.resource_type }} resource(s) stuck for extended period"
```

## Queries for Dashboards

**Real-time pending reconciliations:**
```promql
hyperfleet_api_resource_pending_reconciliation{resource_type="cluster"}
```

**Stuck resources with duration:**
```promql
hyperfleet_api_resource_pending_reconciliation_stuck{resource_type="cluster"}
hyperfleet_api_resource_pending_reconciliation_stuck_duration_seconds{resource_type="cluster"}
```

**Total reconciliations entered (cumulative):**
```promql
hyperfleet_api_resource_pending_reconciliation_total{resource_type="cluster"}
```

## Files to Modify

1. **New**: `pkg/metrics/reconciliation.go` — Counter + Collector
2. **New**: `pkg/metrics/reconciliation_test.go` — Unit tests
3. **Edit**: `pkg/config/metrics.go` — Add `ReconciliationStuckThreshold`
4. **Edit**: `pkg/config/loader.go` — Bind reconciliation_stuck_threshold flag
5. **Edit**: `pkg/services/cluster.go` — Add metric call when True→False
6. **Edit**: `pkg/services/node_pool.go` — Add metric call when True→False
7. **Edit**: `cmd/hyperfleet-api/servecmd/cmd.go` — Register collector
8. **Edit**: `docs/metrics.md` — Document new metrics
9. **Edit**: `charts/templates/prometheusrule.yaml` — Add alerts
10. **Edit**: `charts/values.yaml` — Add reconciliation_stuck_threshold config

## Priority: Medium

Complements deletion observability. Detects reconciliation failures independently of deletion flow.
