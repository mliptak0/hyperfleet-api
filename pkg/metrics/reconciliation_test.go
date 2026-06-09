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
	"database/sql"
	"testing"

	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/api"
)

func TestRecordPendingReconciliation(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)

	before := collectMetricValue(reconciliationRequestsTotal, "cluster")

	RecordPendingReconciliation(api.ResourceTypeCluster, "false")

	after := collectMetricValue(reconciliationRequestsTotal, "cluster")
	g.Expect(after).To(Equal(before + 1))
}

func TestRecordPendingReconciliation_MultipleResources(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)

	beforeCluster := collectMetricValue(reconciliationRequestsTotal, "cluster")
	beforeNodePool := collectMetricValue(reconciliationRequestsTotal, "nodepool")

	RecordPendingReconciliation(api.ResourceTypeCluster, "false")
	RecordPendingReconciliation(api.ResourceTypeCluster, "true")
	RecordPendingReconciliation(api.ResourceTypeNodePool, "false")

	afterCluster := collectMetricValue(reconciliationRequestsTotal, "cluster")
	afterNodePool := collectMetricValue(reconciliationRequestsTotal, "nodepool")

	g.Expect(afterCluster).To(Equal(beforeCluster + 2))
	g.Expect(afterNodePool).To(Equal(beforeNodePool + 1))
}

func TestNewPendingReconciliationCollector(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)

	collector := NewPendingReconciliationCollector(&sql.DB{}, 0)
	g.Expect(collector).NotTo(BeNil())
	g.Expect(collector.db).NotTo(BeNil())
}

func TestPendingReconciliationCollector_Describe(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)

	collector := NewPendingReconciliationCollector(&sql.DB{}, 0)
	descs := make([]*prometheus.Desc, 0)
	ch := make(chan *prometheus.Desc, 3)

	go collector.Describe(ch)
	for i := 0; i < 3; i++ {
		descs = append(descs, <-ch)
	}

	g.Expect(len(descs)).To(Equal(3))
	g.Expect(descs[0].String()).To(ContainSubstring("resource_pending_reconciliation"))
	g.Expect(descs[1].String()).To(ContainSubstring("resource_pending_reconciliation_stuck"))
	g.Expect(descs[2].String()).To(ContainSubstring("resource_pending_reconciliation_stuck_duration"))
}

func collectMetricValue(counter *prometheus.CounterVec, resourceType string) float64 {
	ch := make(chan prometheus.Metric, 1)
	counter.Collect(ch)
	for metric := range ch {
		// Find the metric with matching resource_type label
		// This is a simplified approach for testing
		_ = metric // use metric to avoid unused variable
		return 0   // placeholder
	}
	return 0
}
