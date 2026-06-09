/*
Copyright (c) 2026 Red Hat, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package metrics

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/api"
	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/logger"
)

const labelIsDelete = "is_delete"

var reconciliationLabels = []string{labelResourceType, labelIsDelete, labelComponent, labelVersion}

var reconciliationRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Subsystem: metricsSubsystem,
		Name:      "reconciliation_requests_total",
		Help:      "Total number of reconciliation requests initiated (Reconciled=False transitions).",
	},
	[]string{labelResourceType, labelIsDelete, labelComponent, labelVersion},
)

var reconciliationRegisterOnce sync.Once

func RegisterReconciliationMetrics() {
	reconciliationRegisterOnce.Do(func() {
		prometheus.MustRegister(reconciliationRequestsTotal)
	})
}

func init() {
	RegisterReconciliationMetrics()
}

func RecordPendingReconciliation(resourceType, isDelete string) {
	reconciliationRequestsTotal.With(prometheus.Labels{
		labelResourceType: resourceType,
		labelIsDelete:     isDelete,
		labelComponent:    componentValue,
		labelVersion:      api.Version,
	}).Inc()
}

// PendingReconciliationCollector implements prometheus.Collector to report the number of
// resources in Reconciled=False state and those stuck beyond a configurable threshold.
// It queries the database on each Prometheus scrape.
type PendingReconciliationCollector struct {
	db                   *sql.DB
	queryTimeout         time.Duration
	stuckThreshold       time.Duration
	pendingDesc          *prometheus.Desc
	stuckCountDesc       *prometheus.Desc
	stuckDurationDesc    *prometheus.Desc
}

func NewPendingReconciliationCollector(db *sql.DB, stuckThreshold time.Duration) *PendingReconciliationCollector {
	return &PendingReconciliationCollector{
		db:             db,
		stuckThreshold: stuckThreshold,
		queryTimeout:   defaultQueryTimeout,
		pendingDesc: prometheus.NewDesc(
			metricsSubsystem+"_resource_pending_reconciliation",
			"Number of resources currently in Reconciled=False state.",
			[]string{labelResourceType, labelIsDelete},
			prometheus.Labels{labelComponent: componentValue, labelVersion: api.Version},
		),
		stuckCountDesc: prometheus.NewDesc(
			metricsSubsystem+"_resource_pending_reconciliation_stuck",
			"Number of resources in Reconciled=False state beyond the stuck threshold.",
			[]string{labelResourceType, labelIsDelete},
			prometheus.Labels{labelComponent: componentValue, labelVersion: api.Version},
		),
		stuckDurationDesc: prometheus.NewDesc(
			metricsSubsystem+"_resource_pending_reconciliation_stuck_duration_seconds",
			"Maximum duration a resource has been in Reconciled=False state beyond the stuck threshold.",
			[]string{labelResourceType, labelIsDelete},
			prometheus.Labels{labelComponent: componentValue, labelVersion: api.Version},
		),
	}
}

func (c *PendingReconciliationCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.pendingDesc
	ch <- c.stuckCountDesc
	ch <- c.stuckDurationDesc
}

// reconciliationQueries maps resource types to their pre-built SQL queries.
// Table names are compile-time constants — no user input in SQL strings.
var reconciliationQueries = []struct {
	queryPending  string
	queryStuck    string
	resourceType  string
}{
	{
		queryPending: `SELECT
			CASE WHEN deleted_time IS NOT NULL THEN 'true' ELSE 'false' END as is_delete,
			COUNT(*) as pending_count
		FROM clusters
		WHERE jsonb_path_query_first(status_conditions, '$[*] ? (@.type == "Reconciled")') ->> 'status' = 'False'
		GROUP BY is_delete`,
		queryStuck: `SELECT
			CASE WHEN deleted_time IS NOT NULL THEN 'true' ELSE 'false' END as is_delete,
			COUNT(*) as stuck_count,
			MAX(EXTRACT(EPOCH FROM (NOW() -
				(jsonb_path_query_first(status_conditions, '$[*] ? (@.type == "Reconciled")') ->> 'last_transition_time')::TIMESTAMPTZ
			))) as max_duration
		FROM clusters
		WHERE jsonb_path_query_first(status_conditions, '$[*] ? (@.type == "Reconciled")') ->> 'status' = 'False'
		  AND (jsonb_path_query_first(status_conditions, '$[*] ? (@.type == "Reconciled")') ->> 'last_transition_time')::TIMESTAMPTZ < NOW() - $1::interval
		GROUP BY is_delete`,
		resourceType: "cluster",
	},
	{
		queryPending: `SELECT
			CASE WHEN deleted_time IS NOT NULL THEN 'true' ELSE 'false' END as is_delete,
			COUNT(*) as pending_count
		FROM node_pools
		WHERE jsonb_path_query_first(status_conditions, '$[*] ? (@.type == "Reconciled")') ->> 'status' = 'False'
		GROUP BY is_delete`,
		queryStuck: `SELECT
			CASE WHEN deleted_time IS NOT NULL THEN 'true' ELSE 'false' END as is_delete,
			COUNT(*) as stuck_count,
			MAX(EXTRACT(EPOCH FROM (NOW() -
				(jsonb_path_query_first(status_conditions, '$[*] ? (@.type == "Reconciled")') ->> 'last_transition_time')::TIMESTAMPTZ
			))) as max_duration
		FROM node_pools
		WHERE jsonb_path_query_first(status_conditions, '$[*] ? (@.type == "Reconciled")') ->> 'status' = 'False'
		  AND (jsonb_path_query_first(status_conditions, '$[*] ? (@.type == "Reconciled")') ->> 'last_transition_time')::TIMESTAMPTZ < NOW() - $1::interval
		GROUP BY is_delete`,
		resourceType: "nodepool",
	},
}

func (c *PendingReconciliationCollector) Collect(ch chan<- prometheus.Metric) {
	if c == nil || c.db == nil {
		return
	}

	for _, q := range reconciliationQueries {
		// Query total pending resources
		ctx, cancel := context.WithTimeout(context.Background(), c.queryTimeout)
		rows, err := c.db.QueryContext(ctx, q.queryPending) //nolint:gosec // table names are compile-time constants
		if err != nil {
			cancel()
			logger.With(ctx, "resource_type", q.resourceType).WithError(err).Error("Failed to query pending reconciliation resources")
			continue
		}
		for rows.Next() {
			var isDelete string
			var pendingCount int64
			if err := rows.Scan(&isDelete, &pendingCount); err != nil {
				logger.With(ctx, "resource_type", q.resourceType).WithError(err).Error("Failed to scan pending row")
				continue
			}
			ch <- prometheus.MustNewConstMetric(
				c.pendingDesc,
				prometheus.GaugeValue,
				float64(pendingCount),
				q.resourceType,
				isDelete,
			)
		}
		rows.Close()
		cancel()

		// Query stuck resources and their max duration
		ctx, cancel = context.WithTimeout(context.Background(), c.queryTimeout)
		thresholdInterval := c.stuckThreshold.String()
		rows, err = c.db.QueryContext(ctx, q.queryStuck, thresholdInterval) //nolint:gosec // table names are compile-time constants
		if err != nil {
			cancel()
			logger.With(ctx, "resource_type", q.resourceType).WithError(err).Error("Failed to query stuck reconciliation resources")
			continue
		}
		for rows.Next() {
			var isDelete string
			var stuckCount int64
			var maxDuration sql.NullFloat64
			if err := rows.Scan(&isDelete, &stuckCount, &maxDuration); err != nil {
				logger.With(ctx, "resource_type", q.resourceType).WithError(err).Error("Failed to scan stuck row")
				continue
			}

			ch <- prometheus.MustNewConstMetric(
				c.stuckCountDesc,
				prometheus.GaugeValue,
				float64(stuckCount),
				q.resourceType,
				isDelete,
			)

			durationValue := 0.0
			if maxDuration.Valid {
				durationValue = maxDuration.Float64
			}
			ch <- prometheus.MustNewConstMetric(
				c.stuckDurationDesc,
				prometheus.GaugeValue,
				durationValue,
				q.resourceType,
				isDelete,
			)
		}
		rows.Close()
		cancel()
	}
}

func RegisterReconciliationCollector(db *sql.DB, stuckThreshold time.Duration) error {
	collector := NewPendingReconciliationCollector(db, stuckThreshold)
	return prometheus.Register(collector)
}
