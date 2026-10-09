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
	// A DM already in its own active stabilization window holds a slot (its
	// previous revision's Deployment is the one the budget bounds). A new spec on
	// that DM must reuse that slot, not wait behind itself — otherwise it would
	// deadlock when the budget is full of its own window. Admit it directly; the
	// window is ended after admission in reconcileCandidatePath.
	if r.inStabilizationWindow(dm) {
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
// Two-phase read to keep the hot path cheap: a queued DM re-decides every
// rolloutQueuedRequeue (~10s), so paying an uncached APIReader List on every such
// reconcile would hammer the API server with one List per queued DM per 10s.
// Instead the decision is first made from the CACHED List; if it would QUEUE, that
// is returned directly (a stale cache can only make a DM wait one extra cycle,
// never over-admit, so queuing needs no uncached confirmation). Only when the
// cached decision would ADMIT do we re-confirm against the UNCACHED List — read
// under budgetMu together with the count+decide+reserve so an admission a
// just-admitted DM already took (a different leader, or this leader's own
// not-yet-cached write) is always counted — so the hard cross-leader limit still
// holds, now paid only at the moment of admission rather than on every queued tick.
func (r *DecisionModelReconciler) rolloutBudgetBlocks(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev string,
) (bool, ctrl.Result, error) {
	if r.MaxConcurrentRollouts <= 0 {
		return false, ctrl.Result{}, nil // unlimited
	}

	// Phase 1: decide from the cached List (cheap, served from the informer).
	cached := &decisionmodelv1alpha1.DecisionModelList{}
	if err := r.listInScope(ctx, cached); err != nil {
		return false, ctrl.Result{}, err
	}
	r.budgetMu.Lock()
	admit := r.budgetWouldAdmitLocked(cached, dm)
	r.budgetMu.Unlock()
	if !admit {
		// Over budget on the cached view: queue. A stale cache can only delay, not
		// over-admit, so no uncached confirmation is needed here (the common hot
		// path for a fleet of queued DMs).
		return true, r.queueRollout(ctx, dm), nil
	}

	// Phase 2: the cached view would admit — re-confirm against an UNCACHED List so
	// a slot a just-admitted DM already took (another leader, or this leader's
	// own write the cache has not caught up to) is counted.
	//
	// The authoritative List MUST be read while holding budgetMu, together with the
	// count+decide+reserve. releaseReservation also takes budgetMu and runs only
	// after a candidate's admission is durable, so holding the lock across the List
	// read serializes it against every release: once we hold the lock no release is
	// in flight, and an uncached (strongly consistent) List taken under the lock
	// observes every durable candidateRevision whose reservation has already been
	// dropped. Reading the List before the lock (as an earlier version did) left a
	// window where a List snapshot could predate another DM's durable write *and*
	// its reservation-release, counting that in-flight rollout as neither active nor
	// reserved — admitting a slot over the budget under heavy parallel reconciles.
	r.budgetMu.Lock()
	defer r.budgetMu.Unlock()
	list := &decisionmodelv1alpha1.DecisionModelList{}
	if err := r.listInScopeUncached(ctx, list); err != nil {
		return false, ctrl.Result{}, err
	}
	return r.decideRolloutBudgetLocked(ctx, list, dm, rev)
}

// budgetWouldAdmitLocked reports whether dm would be admitted given the supplied
// List, without mutating any reservation. It is the read-only predicate used for
// the cheap cached phase-1 check. Caller holds budgetMu.
func (r *DecisionModelReconciler) budgetWouldAdmitLocked(
	list *decisionmodelv1alpha1.DecisionModelList,
	dm *decisionmodelv1alpha1.DecisionModel,
) bool {
	active, waiting := r.countBudgetLocked(list, dm)
	slots := r.MaxConcurrentRollouts - active
	if slots <= 0 {
		return false
	}
	return r.rankAmongWaiting(waiting, dm) < slots
}

// decideRolloutBudgetLocked runs the authoritative count+decide+reserve against
// the (uncached) List. It syncs reservations, admits dm (taking a reservation) or
// queues it. Caller holds budgetMu.
func (r *DecisionModelReconciler) decideRolloutBudgetLocked(
	ctx context.Context,
	list *decisionmodelv1alpha1.DecisionModelList,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev string,
) (bool, ctrl.Result, error) {
	// Reconcile reservations against the freshly listed state: drop any whose DM
	// now visibly carries that candidate (admission durable), or that is gone.
	r.syncReservationsLocked(list)

	active, waiting := r.countBudgetLocked(list, dm)
	slots := r.MaxConcurrentRollouts - active
	if slots <= 0 {
		return true, r.queueRollout(ctx, dm), nil
	}
	if r.rankAmongWaiting(waiting, dm) < slots {
		// Admitted: reserve the slot until status.candidateRevision is visible.
		if r.budgetReservations == nil {
			r.budgetReservations = map[string]string{}
		}
		r.budgetReservations[dm.Namespace+"/"+dm.Name] = rev
		return false, ctrl.Result{}, nil
	}
	return true, r.queueRollout(ctx, dm), nil
}

// budgetQueued is a queued DecisionModel's key and FIFO timestamp.
type budgetQueued struct {
	key string
	t   int64
}

// countBudgetLocked counts the active rollouts in the fleet (excluding dm) and
// collects the DecisionModels waiting ahead of/alongside dm. Active = a DM with a
// durable candidate, inside its stabilization window, or holding a reservation; a
// reservation for a DM absent from the (possibly lagged) List also counts. Caller
// holds budgetMu.
func (r *DecisionModelReconciler) countBudgetLocked(
	list *decisionmodelv1alpha1.DecisionModelList,
	dm *decisionmodelv1alpha1.DecisionModel,
) (active int, waiting []budgetQueued) {
	self := dm.Namespace + "/" + dm.Name
	seen := map[string]struct{}{}
	for i := range list.Items {
		d := &list.Items[i]
		key := d.Namespace + "/" + d.Name
		seen[key] = struct{}{}
		if key == self {
			continue // dm handled separately by the caller
		}
		if d.Status.CandidateRevision != nil {
			active++
			continue
		}
		// A DM still inside its post-promotion stabilization window keeps its
		// previous revision's Deployment running — the second GPU/disk the budget
		// bounds — so it counts as an active rollout until the window ends.
		if r.inStabilizationWindow(d) {
			active++
			continue
		}
		// A same-process reservation (admitted-but-not-yet-persisted candidate)
		// still occupies a slot.
		if _, reserved := r.budgetReservations[key]; reserved {
			active++
			continue
		}
		if d.Status.Phase == decisionmodelv1alpha1.PhasePending {
			waiting = append(waiting, budgetQueued{key: key, t: transitionNanos(d)})
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
	return active, waiting
}

// rankAmongWaiting returns dm's FIFO rank (0-based) among the waiting
// DecisionModels, by phaseTransitionTime with a stable key tiebreak. dm's own
// timestamp is its current Pending transition, or now when it is entering the
// queue this reconcile.
func (r *DecisionModelReconciler) rankAmongWaiting(
	waiting []budgetQueued,
	dm *decisionmodelv1alpha1.DecisionModel,
) int {
	self := dm.Namespace + "/" + dm.Name
	selfQ := budgetQueued{key: self, t: r.now().UnixNano()}
	if dm.Status.Phase == decisionmodelv1alpha1.PhasePending && dm.Status.PhaseTransitionTime != nil {
		selfQ.t = dm.Status.PhaseTransitionTime.UnixNano()
	}
	all := append(append([]budgetQueued{}, waiting...), selfQ)
	sort.Slice(all, func(i, j int) bool {
		if all[i].t != all[j].t {
			return all[i].t < all[j].t
		}
		return all[i].key < all[j].key // stable tiebreak
	})
	for i := range all {
		if all[i].key == selfQ.key {
			return i
		}
	}
	return len(all) // unreachable
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
	return r.listInScopeWith(ctx, r.Client, list)
}

// listInScopeUncached is listInScope through the uncached APIReader, so a
// just-persisted admission (status.candidateRevision) is never missed by a lagging
// manager cache — the cross-leader correctness read for the rollout budget. It
// requires no new RBAC (the operator already lists/watches DecisionModels) and
// runs only when the cheap cached phase-1 check would ADMIT, to confirm the slot
// is really free — never on a queued DM's steady-state re-decide (which stays on
// the cached List), so a fleet of queued DecisionModels does not hammer the API
// server with uncached Lists every requeue.
func (r *DecisionModelReconciler) listInScopeUncached(
	ctx context.Context,
	list *decisionmodelv1alpha1.DecisionModelList,
) error {
	if r.APIReader == nil {
		// Fall back to the cached client rather than fail; the manager always
		// injects APIReader in production (SetupWithManager), so this only matters
		// in a unit test that did not set it.
		return r.listInScope(ctx, list)
	}
	return r.listInScopeWith(ctx, r.APIReader, list)
}

// listInScopeWith lists DecisionModels across the watched scope using the given
// reader (the cached client or the uncached APIReader).
func (r *DecisionModelReconciler) listInScopeWith(
	ctx context.Context,
	reader client.Reader,
	list *decisionmodelv1alpha1.DecisionModelList,
) error {
	if len(r.WatchNamespaces) == 0 {
		return reader.List(ctx, list)
	}
	for _, ns := range r.WatchNamespaces {
		part := &decisionmodelv1alpha1.DecisionModelList{}
		if err := reader.List(ctx, part, client.InNamespace(ns)); err != nil {
			return err
		}
		list.Items = append(list.Items, part.Items...)
	}
	return nil
}
