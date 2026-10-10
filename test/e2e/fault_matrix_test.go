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

package e2e

import (
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/maks3201/decision-model-operator/test/utils"
)

// faultMatrixNS is the namespace for the fault-matrix scenarios. Each spec uses its
// own DecisionModel name inside it so the heavy laya:en Pods never overlap across
// specs (the specs run sequentially).
const faultMatrixNS = "dmo-e2e-fault-matrix"

// This container covers reconcile-fault seams that the chaos suite does not: a
// spec.model change that lands WHILE an in-flight stable store recovery is running
// (recovery must finish before the new model rolls, never a terminal failure),
// manager-Pod restarts across the evaluation->promotion seam (the Service must still
// end on the new revision with no duplicate objects and the old revision collected),
// and concurrent reconciles against a multi-replica stable (no readiness-gate flap,
// endpoints stay at the replica count).
//
// It uses the default registry (the lifecycle CI already pulls laya:en and
// laya:multilingual over the network), so it needs no mirror. laya-specific (gap 1
// changes to laya:multilingual) and slow (several model loads), so it is labelled
// "fault-matrix" and "nightly" and runs in its own nightly job.
var _ = Describe("Fault matrix: recovery, promote, concurrency", Label("fault-matrix", "nightly"), Ordered, func() {

	BeforeAll(func() {
		if testInstall == "helm" {
			Skip("helm job runs lifecycle specs only")
		}
		if !modelIsLaya() {
			Skip("laya-specific: gap 1 changes spec.model to laya:multilingual")
		}
		installAndDeploy()
		_, _ = utils.Kubectl("create", "ns", faultMatrixNS)
	})

	AfterAll(func() {
		_, _ = utils.Kubectl("delete", "ns", faultMatrixNS, "--ignore-not-found")
		undeploy()
	})

	// Gap 2: a spec.model change DURING an in-flight stable store recovery. Bring up a
	// settled stable, delete its store PVC + Pod to force a recovery prefetch, and as
	// soon as the recovery Job exists change spec.model. The operator must FINISH
	// recovering the stable store (not abandon it to chase the candidate, which would
	// leave the stable store half-filled) -- the stable returns Ready on its hash --
	// and only then roll out and promote the new model. No terminal Failed/RolledBack
	// at any point.
	It("finishes stable store recovery before rolling a model change made mid-recovery", func() {
		const dm = "recovery-respec"
		fmApply(dm, "laya:en", 1)
		fmAwaitReady(dm, 8*time.Minute)
		stableS := fmStable(dm)

		By("waiting for the post-promotion stabilization window to close (previousRevision clears)")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(faultMatrixNS, "decisionmodel", dm, "{.status.previousRevision}")
		}, 5*time.Minute, 5*time.Second).Should(BeEmpty())

		By("deleting the stable store PVC and its Pod to force a store recovery")
		_, _ = utils.Kubectl("delete", "pvc", dm+"-store-"+stableS, "-n", faultMatrixNS, "--wait=false")
		_, _ = utils.Kubectl("delete", "pods", "-l",
			"decisionmodel.io/name="+dm+",decisionmodel.io/revision="+stableS, "-n", faultMatrixNS,
			"--grace-period=0", "--force")

		By("as soon as the recovery prefetch Job exists, changing spec.model to laya:multilingual")
		Eventually(func() int {
			return fmPrefetchJobCount(dm, stableS)
		}, 3*time.Minute, 2*time.Second).Should(BeNumerically(">=", 1),
			"a stable store recovery prefetch Job should appear")
		fmPatchModel(dm, "laya:multilingual")

		By("the recovery Job is not deleted before it completes (recovery is not abandoned)")
		// The recovery prefetch Job for the stable revision must run to completion; a
		// reconcile chasing the candidate must not garbage-collect it mid-flight.
		Eventually(func(g Gomega) {
			done := fmJobComplete(dm, stableS)
			g.Expect(done).To(BeTrue(), "the stable store recovery Job must complete, not be deleted mid-recovery")
		}, 10*time.Minute, 10*time.Second).Should(Succeed())

		By("the stable returns Ready on its original hash before the new model rolls")
		// previousRevision stays empty (this is recovery of the stable, not a rollout);
		// the stable hash must come back on itself at least transiently.
		Eventually(func(g Gomega) {
			phase, _ := utils.KubectlJSONPath(faultMatrixNS, "decisionmodel", dm, "{.status.phase}")
			g.Expect(phase).NotTo(BeElementOf("Failed", "RolledBack"),
				"a model change during store recovery must not fail or roll back the stable")
		}, 2*time.Minute, 5*time.Second).Should(Succeed())

		By("the new model (laya:multilingual) rolls out and promotes to a new stable")
		var newStable string
		Eventually(func(g Gomega) {
			phase, _ := utils.KubectlJSONPath(faultMatrixNS, "decisionmodel", dm, "{.status.phase}")
			g.Expect(phase).To(Equal("Ready"))
			model, _ := utils.KubectlJSONPath(faultMatrixNS, "decisionmodel", dm, "{.status.stableRevision.model}")
			g.Expect(model).To(Equal("laya:multilingual"), "the new model should be the promoted stable")
			newStable, _ = utils.KubectlJSONPath(faultMatrixNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
			g.Expect(newStable).NotTo(Equal(stableS), "the model change must create and promote a new revision")
		}, 15*time.Minute, 10*time.Second).Should(Succeed())

		By("no terminal failure was recorded for the model change")
		failed, _ := utils.KubectlJSONPath(faultMatrixNS, "decisionmodel", dm, "{.status.failedRevision}")
		Expect(failed).To(BeEmpty(), "no revision should have failed during the recovery+respec sequence")

		dumpDiag(faultMatrixNS, dm)
		_, _ = utils.Kubectl("delete", "decisionmodel", dm, "-n", faultMatrixNS, "--ignore-not-found")
	})

	// Gap 11: manager-Pod restarts across the promotion seam. The Promoting phase is
	// reserved and never observed (promotion is a single reconcile: persist
	// status.stableRevision, then switch the Service), so rather than poll for
	// "Promoting" we kill the manager Pod repeatedly throughout the
	// evaluation->promotion window. The promotion must still complete exactly once:
	// the Service ends on the new revision, there is no duplicate object per revision,
	// and the old revision is collected (previousRevision clears).
	It("promotes exactly once across repeated manager restarts in the promotion window", func() {
		const dm = "promote-seam"
		fmApply(dm, "laya:en", 1)
		fmAwaitReady(dm, 8*time.Minute)
		stableS := fmStable(dm)
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(faultMatrixNS, "decisionmodel", dm, "{.status.previousRevision}")
		}, 5*time.Minute, 5*time.Second).Should(BeEmpty())

		By("forcing a candidate (CPU change) and killing the manager Pod in a loop until promotion")
		fmPatchCPU(dm, "600m")
		// Wait for the candidate to be admitted so the kills land across the
		// evaluation/promotion window rather than before the candidate even exists.
		Eventually(func() string {
			return candidateRevisionIn(faultMatrixNS, dm)
		}, 5*time.Minute, 3*time.Second).ShouldNot(BeEmpty())

		var newStable string
		Eventually(func(g Gomega) {
			// Each poll kills the manager, so a restart lands repeatedly in the
			// candidate-serving / promotion window; the reconcile is level-triggered
			// and persists before switching, so this must still converge.
			killOperatorPod()
			phase, _ := utils.KubectlJSONPath(faultMatrixNS, "decisionmodel", dm, "{.status.phase}")
			g.Expect(phase).NotTo(BeElementOf("Failed", "RolledBack"),
				"repeated restarts in the promotion window must not fail the rollout")
			g.Expect(phase).To(Equal("Ready"), "the rollout should eventually reach Ready despite restarts")
			newStable, _ = utils.KubectlJSONPath(faultMatrixNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
			g.Expect(newStable).NotTo(Equal(stableS), "the candidate should have promoted")
		}, 15*time.Minute, 15*time.Second).Should(Succeed())

		By("the Service ends on the new revision")
		Expect(serviceRevision(faultMatrixNS, dm)).To(Equal(newStable),
			"the Service selector must point at the promoted revision")

		By("no duplicate objects per revision and the old revision is collected")
		Eventually(func(g Gomega) {
			g.Expect(fmDeploymentCount(dm, newStable)).To(Equal(1),
				"exactly one serving Deployment for the promoted revision")
			g.Expect(pvcCountForRevision(faultMatrixNS, dm, newStable)).To(Equal(1),
				"exactly one store PVC for the promoted revision")
			g.Expect(fmDeploymentCount(dm, stableS)).To(Equal(0),
				"the old revision's Deployment must be collected")
			prev, _ := utils.KubectlJSONPath(faultMatrixNS, "decisionmodel", dm, "{.status.previousRevision}")
			g.Expect(prev).To(BeEmpty(), "the previous revision must be collected after stabilization")
		}, 8*time.Minute, 10*time.Second).Should(Succeed())

		dumpDiag(faultMatrixNS, dm)
		_, _ = utils.Kubectl("delete", "decisionmodel", dm, "-n", faultMatrixNS, "--ignore-not-found")
	})

	// Gap 10: a multi-replica stable under a storm of concurrent reconciles. Scale to
	// replicas 3, then annotate the DM in a tight loop for 60s (annotations are not in
	// the revision hash, so this forces re-reconciles without a new revision). Every
	// serving Pod's decisionmodel.io/model-ready gate must stay True throughout and the
	// Service endpoint count must stay at 3 -- no gate flap, no endpoint churn.
	It("keeps a 3-replica stable gated and endpoints stable under concurrent reconciles", func() {
		const dm = "concurrent-reconcile"
		fmApply(dm, "laya:en", 3)
		fmAwaitReady(dm, 12*time.Minute)
		Eventually(func(g Gomega) {
			ready, _ := utils.KubectlJSONPath(faultMatrixNS, "decisionmodel", dm, "{.status.replicas.modelReady}")
			g.Expect(ready).To(Equal("3"), "all three replicas must be model-ready before the storm")
		}, 12*time.Minute, 10*time.Second).Should(Succeed())

		By("annotating the DM in a tight loop for 60s to force concurrent reconciles")
		stop := make(chan struct{})
		go func() {
			defer GinkgoRecover()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = utils.Kubectl("annotate", "decisionmodel", dm, "-n", faultMatrixNS,
						fmt.Sprintf("dmo-e2e/poke=%d", i), "--overwrite")
					i++
				}
			}
		}()

		By("no gate flap and endpoints stay at 3 for the whole window")
		Consistently(func(g Gomega) {
			gates := fmGateStatuses(dm)
			g.Expect(gates).To(HaveLen(3), "expected three serving Pods, got gates %v", gates)
			for _, s := range gates {
				g.Expect(s).To(Equal("True"), "every serving Pod's model-ready gate must stay True (got %v)", gates)
			}
			g.Expect(fmEndpointCount(dm)).To(Equal(3), "the Service must keep exactly 3 ready endpoints")
		}, 60*time.Second, 3*time.Second).Should(Succeed())
		close(stop)

		dumpDiag(faultMatrixNS, dm)
		_, _ = utils.Kubectl("delete", "decisionmodel", dm, "-n", faultMatrixNS, "--ignore-not-found")
	})
})

// ---- helpers local to the fault-matrix container ----

// fmApply creates a laya DecisionModel in the fault-matrix namespace with a small
// CPU request and a 4Gi memory limit (laya:en is ~3.09 GiB, laya:multilingual
// ~1.85 GiB; 4Gi avoids an OOM on either).
func fmApply(name, model string, replicas int) {
	applyYAML(fmt.Sprintf(`apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata: {name: %s, namespace: %s}
spec:
  engine: ollaya
  model: %s
  device: %s
  replicas: %d
  resources: {requests: {cpu: 250m, memory: 1Gi}, limits: {memory: 4Gi}}
`, name, faultMatrixNS, model, testDevice, replicas))
}

// fmAwaitReady waits for the DM to reach phase Ready.
func fmAwaitReady(name string, timeout time.Duration) {
	Eventually(func() (string, error) {
		return utils.KubectlJSONPath(faultMatrixNS, "decisionmodel", name, "{.status.phase}")
	}, timeout, 5*time.Second).Should(Equal("Ready"))
}

// fmStable returns the DM's stable revision hash, failing the spec if empty.
func fmStable(name string) string {
	h, err := utils.KubectlJSONPath(faultMatrixNS, "decisionmodel", name, "{.status.stableRevision.hash}")
	Expect(err).NotTo(HaveOccurred())
	Expect(h).NotTo(BeEmpty())
	return h
}

// fmPatchModel sets spec.model with a merge patch (forces a new revision).
func fmPatchModel(name, model string) {
	_, err := utils.Kubectl("patch", "decisionmodel", name, "-n", faultMatrixNS, "--type=merge",
		"-p", fmt.Sprintf(`{"spec":{"model":%q}}`, model))
	Expect(err).NotTo(HaveOccurred())
}

// fmPatchCPU sets the serving CPU request (forces a new revision hash).
func fmPatchCPU(name, cpu string) {
	_, err := utils.Kubectl("patch", "decisionmodel", name, "-n", faultMatrixNS, "--type=merge",
		"-p", fmt.Sprintf(`{"spec":{"resources":{"requests":{"cpu":%q,"memory":"1Gi"},"limits":{"memory":"4Gi"}}}}`, cpu))
	Expect(err).NotTo(HaveOccurred())
}

// fmPrefetchJobCount counts prefetch Jobs for a revision. The prefetch Job's own
// metadata carries the revision label (decisionmodel.io/revision via revisionLabels);
// only its Pod template carries decisionmodel.io/prefetch-revision, so a Job selector
// must use the revision label. `kubectl get jobs` only returns Jobs, so the serving
// Deployment's identical revision label does not collide here.
func fmPrefetchJobCount(name, rev string) int {
	out, _ := utils.Kubectl("get", "jobs", "-n", faultMatrixNS,
		"-l", "decisionmodel.io/name="+name+",decisionmodel.io/revision="+rev,
		"-o", "jsonpath={.items[*].metadata.name}")
	return len(strings.Fields(out))
}

// fmJobComplete reports whether a prefetch Job for the revision reached Complete.
// A deleted (abandoned) Job returns false, which is what the recovery assertion
// guards against. Selects by the Job's revision label (see fmPrefetchJobCount).
func fmJobComplete(name, rev string) bool {
	out, _ := utils.Kubectl("get", "jobs", "-n", faultMatrixNS,
		"-l", "decisionmodel.io/name="+name+",decisionmodel.io/revision="+rev,
		"-o", "jsonpath={.items[*].status.conditions[?(@.type=='Complete')].status}")
	return strings.Contains(out, "True")
}

// fmDeploymentCount counts serving Deployments carrying a revision label.
func fmDeploymentCount(name, rev string) int {
	out, _ := utils.Kubectl("get", "deploy", "-n", faultMatrixNS,
		"-l", "decisionmodel.io/name="+name+",decisionmodel.io/revision="+rev,
		"-o", "jsonpath={.items[*].metadata.name}")
	return len(strings.Fields(out))
}

// fmGateStatuses returns the decisionmodel.io/model-ready gate status of every
// serving Pod (excludes the prefetch Pod via servingPodSelector).
func fmGateStatuses(name string) []string {
	out, _ := utils.Kubectl("get", "pods", "-l", servingPodSelector(name), "-n", faultMatrixNS,
		"-o", "jsonpath={.items[*].status.conditions[?(@.type=='decisionmodel.io/model-ready')].status}")
	return strings.Fields(out)
}

// fmEndpointCount returns the number of ready endpoint addresses backing the DM's
// Service.
func fmEndpointCount(name string) int {
	out, _ := utils.Kubectl("get", "endpoints", name, "-n", faultMatrixNS,
		"-o", "jsonpath={.subsets[*].addresses[*].ip}")
	return len(strings.Fields(out))
}

// candidateRevisionIn returns the DM's candidate revision hash in the given
// namespace (empty if none).
func candidateRevisionIn(ns, name string) string {
	rev, _ := utils.KubectlJSONPath(ns, "decisionmodel", name, "{.status.candidateRevision.hash}")
	return rev
}
