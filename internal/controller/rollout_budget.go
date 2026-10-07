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
	"fmt"
	"sort"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
)

// rolloutQueuedRequeue is how often a DecisionModel queued behind the fleet
// rollout budget is re-checked for a free slot.
const rolloutQueuedRequeue = 10 * time.Second

// gateRolloutBudget returns queued=true (with its requeue result) when a
// brand-new candidate for dm must wait behind the fleet rollout budget. A
// candidate already admitted (status.candidateRevision.hash == rev) bypasses the
// gate so an in-flight rollout never deadlocks itself.
func (r *DecisionModelReconciler) gateRolloutBudget(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev string,
) (bool, ctrl.Result, error) {
	admitted := dm.Status.CandidateRevision != nil && dm.Status.CandidateRevision.Hash == rev
	if admitted {
		// Already recorded as a candidate: the reservation (if any) is now
		// redundant; drop it so a stale entry cannot double-count.
		r.releaseReservation(dm.Namespace, dm.Name)
		return false, ctrl.Result{}, nil
	}
	return r.rolloutBudgetBlocks(ctx, dm, rev)
}

// rolloutBudgetBlocks reports whether a brand-new candidate for dm must wait
// because the fleet is at --max-concurrent-rollouts. When it must wait it also
// sets phase Pending (reason RolloutQueued) and returns the requeue result.
//
// Admission is race-free for a single manager process (leader election
// guarantees one active reconciler): the whole "count + decide + reserve" runs
// under budgetMu, so two DecisionModels reconciled concurrently cannot both see
// a free slot. The durable record of an admission is status.candidateRevision;
// until the manager cache reflects that write, an in-memory reservation
// (ns/name -> rev) stands in and counts as active, closing the cache-lag window.
//
// The count derives from cluster state (status.candidateRevision) plus the
// reservation set; FIFO is by phaseTransitionTime among queued (Pending)
// DecisionModels plus dm itself. The cached List is sufficient (no uncached
// APIReader): the only staleness that matters is this process's own just-written
// candidateRevision, which the reservation covers until the cache catches up; a
// reservation is dropped once the cached DM shows the candidate.
func (r *DecisionModelReconciler) rolloutBudgetBlocks(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev string,
) (bool, ctrl.Result, error) {
	if r.MaxConcurrentRollouts <= 0 {
		return false, ctrl.Result{}, nil // unlimited
	}

	list := &decisionmodelv1alpha1.DecisionModelList{}
	if err := r.listInScope(ctx, list); err != nil {
		return false, ctrl.Result{}, err
	}

	r.budgetMu.Lock()
	defer r.budgetMu.Unlock()

	// Reconcile reservations against the freshly listed state: drop any whose DM
	// now visibly carries that candidate (admission durable), or that is gone.
	r.syncReservationsLocked(list)

	self := dm.Namespace + "/" + dm.Name
	active := 0
	type queued struct {
		key string
		t   int64
	}
	var waiting []queued
	seen := map[string]struct{}{}
	for i := range list.Items {
		d := &list.Items[i]
		key := d.Namespace + "/" + d.Name
		seen[key] = struct{}{}
		if key == self {
			continue // dm handled separately below
		}
		if d.Status.CandidateRevision != nil {
			active++
			continue
		}
		// An admitted-but-not-yet-visible candidate (its status write has not
		// reached the cache) still occupies a slot via its reservation.
		if _, reserved := r.budgetReservations[key]; reserved {
			active++
			continue
		}
		if d.Status.Phase == decisionmodelv1alpha1.PhasePending {
			waiting = append(waiting, queued{key: key, t: transitionNanos(d)})
		}
	}
	// A reservation for a DM not in this (possibly lagged) List still counts.
	for key := range r.budgetReservations {
		if key == self {
			continue
		}
		if _, ok := seen[key]; !ok {
			active++
		}
	}

	slots := r.MaxConcurrentRollouts - active
	if slots <= 0 {
		return true, r.queueRollout(ctx, dm), nil
	}

	// Rank dm against the other waiting DecisionModels by phaseTransitionTime
	// (FIFO). dm's own timestamp is its current Pending transition, or now when it
	// is entering the queue this reconcile.
	selfQ := queued{key: self, t: r.now().UnixNano()}
	if dm.Status.Phase == decisionmodelv1alpha1.PhasePending && dm.Status.PhaseTransitionTime != nil {
		selfQ.t = dm.Status.PhaseTransitionTime.UnixNano()
	}
	all := append(waiting, selfQ)
	sort.Slice(all, func(i, j int) bool {
		if all[i].t != all[j].t {
			return all[i].t < all[j].t
		}
		return all[i].key < all[j].key // stable tiebreak
	})
	rank := 0
	for i := range all {
		if all[i].key == selfQ.key {
			rank = i
			break
		}
	}
	if rank < slots {
		// Admitted: reserve the slot until status.candidateRevision is visible.
		if r.budgetReservations == nil {
			r.budgetReservations = map[string]string{}
		}
		r.budgetReservations[self] = rev
		return false, ctrl.Result{}, nil
	}
	return true, r.queueRollout(ctx, dm), nil
}

// syncReservationsLocked drops reservations whose admission is now durable and
// visible in the cache: the DM carries that candidate (candidateRevision.hash ==
// rev). It deliberately does NOT drop a reservation just because the DM is absent
// from this List — the List can lag, and dropping on absence would reopen the
// over-admission race the reservation exists to close. Reservations for deleted
// DMs or ended candidates are freed explicitly (releaseReservation / the
// admitted-bypass in gateRolloutBudget). Caller holds budgetMu.
func (r *DecisionModelReconciler) syncReservationsLocked(list *decisionmodelv1alpha1.DecisionModelList) {
	if len(r.budgetReservations) == 0 {
		return
	}
	for i := range list.Items {
		d := &list.Items[i]
		key := d.Namespace + "/" + d.Name
		rev, ok := r.budgetReservations[key]
		if !ok {
			continue
		}
		if d.Status.CandidateRevision != nil && d.Status.CandidateRevision.Hash == rev {
			delete(r.budgetReservations, key) // admission now durable/visible
		}
	}
}

// releaseReservation drops dm's rollout-budget reservation (candidate ended or DM
// deleted), freeing the slot for a queued DecisionModel.
func (r *DecisionModelReconciler) releaseReservation(namespace, name string) {
	r.budgetMu.Lock()
	defer r.budgetMu.Unlock()
	delete(r.budgetReservations, namespace+"/"+name)
}

// queueRollout parks dm in Pending/RolloutQueued and returns the requeue result.
func (r *DecisionModelReconciler) queueRollout(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
) ctrl.Result {
	r.setPhase(ctx, dm, decisionmodelv1alpha1.PhasePending)
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionReady,
		Status:  metav1.ConditionFalse,
		Reason:  reasonRolloutQueued,
		Message: fmt.Sprintf("waiting for a rollout slot (max %d concurrent rollouts)", r.MaxConcurrentRollouts),
	})
	res, _ := r.finish(ctx, dm, ctrl.Result{RequeueAfter: rolloutQueuedRequeue}, nil)
	return res
}

// transitionNanos returns a DecisionModel's phaseTransitionTime in nanoseconds,
// or 0 when unset (sorts first — an un-stamped waiter yields to any stamped one,
// which cannot happen in practice because queueRollout always stamps it).
func transitionNanos(d *decisionmodelv1alpha1.DecisionModel) int64 {
	if d.Status.PhaseTransitionTime == nil {
		return 0
	}
	return d.Status.PhaseTransitionTime.UnixNano()
}

// listInScope lists DecisionModels across the watched scope: cluster-wide when
// no namespaces are configured, else once per watched namespace (the manager
// cache is scoped the same way, and namespaced mode makes no cluster-wide call).
func (r *DecisionModelReconciler) listInScope(
	ctx context.Context,
	list *decisionmodelv1alpha1.DecisionModelList,
) error {
	if len(r.WatchNamespaces) == 0 {
		return r.List(ctx, list)
	}
	for _, ns := range r.WatchNamespaces {
		part := &decisionmodelv1alpha1.DecisionModelList{}
		if err := r.List(ctx, part, client.InNamespace(ns)); err != nil {
			return err
		}
		list.Items = append(list.Items, part.Items...)
	}
	return nil
}
