/*
Copyright 2026 maks3201.

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

package controller

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
)

func budgetTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := decisionmodelv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	return s
}

// listCountingReader wraps a client.Reader and counts List calls for
// DecisionModelLists, so a test can prove the uncached APIReader is NOT consulted
// on a queued DM's steady-state re-decide (only when the cheap cached check would
// admit).
type listCountingReader struct {
	client.Reader
	dmLists int32
}

func (c *listCountingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*decisionmodelv1alpha1.DecisionModelList); ok {
		atomic.AddInt32(&c.dmLists, 1)
	}
	return c.Reader.List(ctx, list, opts...)
}

func dmWithCandidate(ns, name string) *decisionmodelv1alpha1.DecisionModel {
	dm := &decisionmodelv1alpha1.DecisionModel{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
	}
	dm.Status.CandidateRevision = &decisionmodelv1alpha1.RevisionStatus{Hash: "aaaa"}
	return dm
}

func dmQueued(ns, name string, t time.Time) *decisionmodelv1alpha1.DecisionModel {
	dm := &decisionmodelv1alpha1.DecisionModel{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
	}
	tt := metav1.NewTime(t)
	dm.Status.Phase = decisionmodelv1alpha1.PhasePending
	dm.Status.PhaseTransitionTime = &tt
	return dm
}

// TestRolloutBudgetQueuedUsesCachedReadOnly proves the two-phase budget read: a
// queued DM that is over budget on the cached view is queued WITHOUT any uncached
// APIReader List, so a fleet of queued DMs does not hammer the API server every
// requeue. When a slot is free the cached check would admit, so exactly one
// uncached List runs to confirm.
func TestRolloutBudgetQueuedUsesCachedReadOnly(t *testing.T) {
	scheme := budgetTestScheme(t)
	now := time.Unix(1_000_000, 0)

	t.Run("over budget: no uncached List", func(t *testing.T) {
		// One active rollout (candidate recorded) fills the single slot; our DM is
		// queued behind it.
		active := dmWithCandidate("ns", "active")
		self := dmQueued("ns", "self", now)
		cached := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(active, self).WithStatusSubresource(active, self).Build()
		uncached := &listCountingReader{Reader: cached}

		r := &DecisionModelReconciler{
			Client:                cached,
			APIReader:             uncached,
			MaxConcurrentRollouts: 1,
			Now:                   func() time.Time { return now },
		}
		block, _, err := r.rolloutBudgetBlocks(context.Background(), self, "bbbb")
		if err != nil {
			t.Fatalf("rolloutBudgetBlocks: %v", err)
		}
		if !block {
			t.Fatalf("expected the queued DM to be blocked (budget full)")
		}
		if n := atomic.LoadInt32(&uncached.dmLists); n != 0 {
			t.Errorf("uncached DecisionModel Lists = %d, want 0 on a queued re-decide", n)
		}
	})

	t.Run("slot free: exactly one uncached confirmation List", func(t *testing.T) {
		// No active rollout: the cached check admits, so the uncached List runs once
		// to confirm the slot across leaders.
		self := dmQueued("ns", "self", now)
		cached := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(self).WithStatusSubresource(self).Build()
		uncached := &listCountingReader{Reader: cached}

		r := &DecisionModelReconciler{
			Client:                cached,
			APIReader:             uncached,
			MaxConcurrentRollouts: 1,
			Now:                   func() time.Time { return now },
		}
		block, _, err := r.rolloutBudgetBlocks(context.Background(), self, "bbbb")
		if err != nil {
			t.Fatalf("rolloutBudgetBlocks: %v", err)
		}
		if block {
			t.Fatalf("expected the DM to be admitted (free slot)")
		}
		if n := atomic.LoadInt32(&uncached.dmLists); n != 1 {
			t.Errorf("uncached DecisionModel Lists = %d, want exactly 1 on admission", n)
		}
	})
}
