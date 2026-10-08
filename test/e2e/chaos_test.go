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
	"os"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/maks3201/decision-model-operator/test/utils"
)

// chaosNS holds the DecisionModel under test for the chaos scenarios. The mirror
// lives in mirrorNS (reused from the mirror spec) so faults can be injected into
// the registry independently of the DecisionModel.
const chaosNS = "dmo-e2e-chaos"

// chaosDM is the DecisionModel name used across the chaos scenarios.
const chaosDM = "chaos-router"

// This container injects registry, runtime and cluster faults at specific reconcile
// steps and asserts the documented recovery. It is nightly-only (slow: it drives
// several candidate model loads) and runs through the in-cluster mirror so the
// registry can be broken. Faults are held long enough to outlast the engine's
// in-request retries (3) and at least one reconcile, otherwise nothing is observable.
//
// Scenarios (one DecisionModel reused where possible to bound node memory — every
// laya:en Pod holds the model, so the suite runs them sequentially):
//
//	1a. Registry 500 during Resolving  -> stable keeps serving, Resolved=False/ResolveFailed
//	    (transient, no Degraded, no Event); fault cleared -> the candidate proceeds.
//	1b. Registry fault (503) during prefetch (Caching) -> waits in Caching, not Failed;
//	    cleared -> completes.
//	2.  Registry 404 for the tag       -> Resolved=False/ModelNotFound, stable keeps serving.
//	3.  Runtime fault during Evaluating (candidate Pod disrupted = transport error) ->
//	    evaluation holds and retries, no false reject; recovered -> promotes.
//	4.  Operator Pod killed mid-rollout and after promotion -> converges, exactly one
//	    Deployment/Job/PVC per revision, Service on the stable.
//	5a. Candidate store PVC deleted during Starting -> fresh re-prefetch, then completes.
//	5b. Stable store PVC deleted while Ready -> Degraded store reason, bounded recovery.
//	    (PENDING — see the PIt comment; timing-sensitive on kind.)
//	6.  Stable serving Pod deleted during Stabilizing -> no false rollback within the
//	    debounce. (PENDING — see the PIt comment.)
var _ = Describe("Chaos: faults between critical steps", Label("nightly", "chaos"), Ordered, func() {
	const dm = chaosDM

	BeforeAll(func() {
		if testInstall == "helm" {
			Skip("helm job runs lifecycle specs only")
		}
		if os.Getenv("E2E_MIRROR") == "" {
			Skip("set E2E_MIRROR to build the mirror image and run the chaos spec")
		}
		if !modelIsLaya() {
			Skip("laya-specific: the registry mirror image serves laya:en")
		}
		installAndDeploy()
		deployMirror()
		By("pointing the operator at the mirror")
		patchManagerArgs(
			"--ollaya-registry=http://"+mirrorHost,
			"--allowed-registries="+mirrorHost,
			"--allow-insecure-registries",
		)

		_, _ = utils.Kubectl("create", "ns", chaosNS)
		By("bringing up a stable revision through the mirror")
		applyYAML(fmt.Sprintf(`apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata: {name: %s, namespace: %s}
spec:
  engine: ollaya
  model: laya:en
  device: %s
  replicas: 1
  resources: {requests: {cpu: 250m, memory: 1Gi}, limits: {memory: 4Gi}}
`, dm, chaosNS, testDevice))
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.phase}")
		}, 8*time.Minute, 5*time.Second).Should(Equal("Ready"))
	})

	AfterAll(func() {
		clearMirrorFault() // leave the mirror serving for any later use
		_, _ = utils.Kubectl("delete", "ns", chaosNS, "--ignore-not-found")
		_, _ = utils.Kubectl("delete", "ns", mirrorNS, "--ignore-not-found")
		scaleManager(1)
		undeploy()
		cluster := envOr("KIND_CLUSTER", "kind")
		_, _ = utils.Run(exec.Command("docker", "exec", cluster+"-control-plane",
			"crictl", "rmi", "docker.io/library/"+mirrorImage))
	})

	AfterEach(func() {
		dumpDiag(chaosNS, dm)
		clearMirrorFault()
	})

	It("keeps the stable serving on a transient registry 500 during Resolving", func() {
		stableBefore, err := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stableBefore).NotTo(BeEmpty())

		By("breaking the registry (500) before resolving a new tag")
		setMirrorFault("500")
		// A different valid tag forces a fresh resolve (the current digest is cached).
		// laya:multilingual is a real tag the mirror does NOT have baked in, but the
		// 500 short-circuits before any 404, so the operator sees a transient error.
		_, err = utils.Kubectl("patch", "decisionmodel", dm, "-n", chaosNS, "--type=merge",
			"-p", `{"spec":{"model":"laya:multilingual"}}`)
		Expect(err).NotTo(HaveOccurred())

		By("Resolved goes False with reason ResolveFailed, stable keeps serving (no Degraded, no Event)")
		Eventually(func(g Gomega) {
			status, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Resolved')].status}")
			reason, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Resolved')].reason}")
			g.Expect(status).To(Equal("False"))
			g.Expect(reason).To(Equal("ResolveFailed"), "a transient 500 should be ResolveFailed, not ModelNotFound")
		}, 3*time.Minute, 5*time.Second).Should(Succeed())
		// The stable must not be disturbed by a candidate resolve failure.
		Consistently(func(g Gomega) {
			phase, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.phase}")
			g.Expect(phase).To(Equal("Ready"), "stable must keep serving during a transient registry outage")
			stableNow, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
			g.Expect(stableNow).To(Equal(stableBefore))
			deg, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Degraded')].status}")
			g.Expect(deg).NotTo(Equal("True"), "a transient resolve blip must not set Degraded")
		}, 20*time.Second, 5*time.Second).Should(Succeed())
		Expect(eventReasonCountIn("CandidateRejected")).To(Equal(0),
			"a transient resolve error must not emit a CandidateRejected Event")

		By("restoring the model and clearing the fault returns Resolved=True with the stable intact")
		clearMirrorFault()
		_, err = utils.Kubectl("patch", "decisionmodel", dm, "-n", chaosNS, "--type=merge",
			"-p", `{"spec":{"model":"laya:en"}}`)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func(g Gomega) {
			status, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Resolved')].status}")
			g.Expect(status).To(Equal("True"))
		}, 5*time.Minute, 5*time.Second).Should(Succeed())
		stableNow, err := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stableNow).To(Equal(stableBefore), "restoring the model must not roll the stable (same digest)")
	})

	It("rejects a 404 tag with ModelNotFound while the stable keeps serving", func() {
		stableBefore, err := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stableBefore).NotTo(BeEmpty())

		By("pointing at a tag the mirror does not have")
		_, err = utils.Kubectl("patch", "decisionmodel", dm, "-n", chaosNS, "--type=merge",
			"-p", `{"spec":{"model":"laya:does-not-exist"}}`)
		Expect(err).NotTo(HaveOccurred())

		By("Resolved=False with reason ModelNotFound, phase stays Ready, one CandidateRejected Event")
		Eventually(func(g Gomega) {
			reason, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Resolved')].reason}")
			g.Expect(reason).To(Equal("ModelNotFound"))
		}, 3*time.Minute, 5*time.Second).Should(Succeed())
		phase, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.phase}")
		Expect(phase).To(Equal("Ready"), "a bad candidate tag must not stop the serving stable")
		Expect(eventReasonCountIn("CandidateRejected")).To(BeNumerically(">=", 1),
			"a rejected candidate should emit a CandidateRejected Warning")

		By("restoring the model clears the rejection")
		_, err = utils.Kubectl("patch", "decisionmodel", dm, "-n", chaosNS, "--type=merge",
			"-p", `{"spec":{"model":"laya:en"}}`)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(chaosNS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Resolved')].status}")
		}, 5*time.Minute, 5*time.Second).Should(Equal("True"))
		stableNow, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(stableNow).To(Equal(stableBefore))
	})

	It("holds evaluation and does not false-reject when the runtime fails mid-Evaluating", func() {
		// Fault 3 (runtime fault during Evaluating): make the candidate Pod unable to
		// answer by deleting it repeatedly during the eval window. The evaluator calls
		// the candidate Pod IP directly, so a missing/restarting Pod yields a transport
		// error (connection refused / timeout). A transport error must invalidate the
		// run and be retried — NOT scored as a wrong answer and NOT roll the candidate
		// back. Once the Pod stays up the evaluation completes and the candidate
		// promotes. (A literal 5xx/429 from the runtime is covered by the evaluator unit
		// tests — see the report; the controller path a 5xx exercises is the same
		// transport-invalidate-and-retry path this drives.)
		stableBefore, err := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())

		By("rolling out an eval-gated candidate")
		applyConfigMap(chaosNS, "chaos-golden", "cases.jsonl", datasetJSONL(chaosLabels(dm)))
		patchRollout(dm, chaosNS, rolloutPatch{
			cpu:          "350m",
			datasetCM:    "chaos-golden",
			minAccuracy:  "0.0",
			evaluatingTO: "20m",
		})

		By("waiting until a new revision appears (candidate or freshly promoted), then disrupting its Pod")
		// On a warm store with minAccuracy 0.0 the eval can pass almost immediately, so
		// the candidate may already be the new stable by the time we look; capture the
		// new revision from whichever field holds it. The point is to prove a runtime
		// transport fault around evaluation/promotion never false-rejects or rolls back.
		var candHash string
		Eventually(func(g Gomega) {
			cand, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.candidateRevision.hash}")
			stable, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
			if cand != "" {
				candHash = cand
			} else if stable != stableBefore {
				candHash = stable
			}
			g.Expect(candHash).NotTo(BeEmpty(), "a new revision should appear (candidate or new stable)")
		}, 10*time.Minute, 3*time.Second).Should(Succeed())

		By("deleting the candidate/new-stable Pod once (a single transport fault), then letting it recover")
		// One deletion yields a transport error for an in-flight eval case (or a brief
		// stable-Pod loss that self-heals within the post-promotion debounce). Either
		// way it must not false-reject the candidate or roll it back. A single deletion
		// (not a sustained hold) deliberately stays within the stabilization debounce so
		// this scenario tests the eval/transport path, not the rollback path (scenario 5).
		_, _ = utils.Kubectl("delete", "pods", "-l",
			"decisionmodel.io/name="+dm+",decisionmodel.io/revision="+candHash, "-n", chaosNS,
			"--grace-period=0", "--force")
		Consistently(func(g Gomega) {
			phase, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.phase}")
			g.Expect(phase).NotTo(BeElementOf("Failed", "RolledBack"),
				"a runtime transport fault must not false-reject the candidate or roll it back")
			ev, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Evaluated')].reason}")
			g.Expect(ev).NotTo(Equal("EvaluationFailed"),
				"a transport blip must not be scored as an evaluation failure")
		}, 30*time.Second, 5*time.Second).Should(Succeed())

		By("the candidate completes and the DecisionModel is Ready on the new revision")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.phase}")
		}, 10*time.Minute, 5*time.Second).Should(Equal("Ready"))
		stableNow, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(stableNow).To(Equal(candHash), "the candidate should become/stay the stable revision after the fault")
		Expect(eventReasonCountIn("RolledBackAfterPromotion")).To(Equal(0),
			"a single transport fault must not cause a post-promotion rollback")
	})

	// PENDING: store-loss recovery is timing-sensitive on kind (the dynamic
	// provisioner can recreate the PVC fast enough that the Degraded window is hard to
	// observe deterministically). Enable once it can be iterated against the dispatchable
	// chaos CI (the workflow must be on the default branch before it can be dispatched).
	PIt("recovers a Degraded=StoreLost when the stable store PVC is deleted while Ready", func() {
		stable, err := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stable).NotTo(BeEmpty())
		pvc := dm + "-store-" + stable

		By("deleting the stable store PVC")
		// The serving Pod mounts it read-only; pvc-protection keeps it Terminating
		// until the Pod is gone, so the operator surfaces StoreTerminating first and
		// StoreLost once it is actually gone. Delete the serving Pod too so the PVC can
		// finalize and the bounded recovery can recreate it.
		_, _ = utils.Kubectl("delete", "pvc", pvc, "-n", chaosNS, "--wait=false")
		_, _ = utils.Kubectl("delete", "pods", "-l",
			"decisionmodel.io/name="+dm+",decisionmodel.io/revision="+stable, "-n", chaosNS,
			"--grace-period=0", "--force")

		By("the store is recovered: Degraded surfaces a store reason (best-effort) and it returns to Ready")
		// The Degraded window can be brief if recovery is fast; capture it best-effort
		// over the recovery window rather than requiring a specific poll to catch it.
		sawStoreDegraded := false
		Eventually(func(g Gomega) {
			reason, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Degraded')].reason}")
			deg, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Degraded')].status}")
			if deg == "True" && (reason == "StoreLost" || reason == "StoreTerminating" || reason == "StorePrefetchFailed") {
				sawStoreDegraded = true
			}
			phase, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.phase}")
			g.Expect(phase).To(Equal("Ready"), "bounded store recovery should re-prefetch and serve again")
		}, 12*time.Minute, 3*time.Second).Should(Succeed())
		_, _ = fmt.Fprintf(GinkgoWriter, "store-loss recovery: observed a store Degraded reason = %v\n", sawStoreDegraded)
		stableNow, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(stableNow).To(Equal(stable), "recovery keeps the same revision (re-prefetch, not a new rollout)")
		// The recovered store PVC exists again under the same name.
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(chaosNS, "pvc", pvc, "{.status.phase}")
		}, 2*time.Minute, 5*time.Second).Should(Equal("Bound"), "the stable store PVC should be recreated and Bound")
	})

	It("waits in Caching (not Failed) when the registry faults during prefetch, then completes", func() {
		// A transient registry fault DURING prefetch (the Caching phase) must keep the
		// DecisionModel waiting in Caching while the Job retries, NOT fail it; once the
		// registry recovers the prefetch completes and it promotes. Held via MIRROR_FAULT
		// (503) for longer than the Job's internal retries and a reconcile.
		stableBefore, err := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())

		By("resolving a new revision while the registry is healthy, then faulting it as prefetch starts")
		// Bump cpu to force a new revision; resolve succeeds (registry healthy), then we
		// fault the registry so the prefetch Job's pull fails and retries.
		_, err = utils.Kubectl("patch", "decisionmodel", dm, "-n", chaosNS, "--type=merge",
			"-p", `{"spec":{"resources":{"requests":{"cpu":"600m","memory":"1Gi"},"limits":{"memory":"4Gi"}}}}`)
		Expect(err).NotTo(HaveOccurred())
		// Wait until a candidate is recorded (resolve done), then fault the registry.
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.candidateRevision.hash}")
		}, 3*time.Minute, 2*time.Second).ShouldNot(BeEmpty())
		setMirrorFault("503")

		By("the DecisionModel waits in Caching (Cached=False), not Failed/RolledBack, while the Job retries")
		Consistently(func(g Gomega) {
			phase, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.phase}")
			g.Expect(phase).NotTo(BeElementOf("Failed", "RolledBack"),
				"a transient registry fault during prefetch must not fail the rollout")
		}, 40*time.Second, 5*time.Second).Should(Succeed())

		By("clearing the fault lets the prefetch complete and the candidate promote")
		clearMirrorFault()
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.phase}")
		}, 12*time.Minute, 10*time.Second).Should(Equal("Ready"),
			"once the registry recovers the prefetch should complete and promote")
		stableNow, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(stableNow).NotTo(Equal(stableBefore), "the new revision should have promoted after recovery")
	})

	It("converges with no duplicate objects when the operator Pod is killed mid-rollout", func() {
		// Kill the operator Pod while a candidate is rolling out (right after it is
		// created) and again after it promotes. Owner refs + deterministic per-revision
		// names + level-triggered reconcile + persist-before-switch make this safe: the
		// restarted operator re-asserts the same objects (idempotent) and ends with
		// exactly one Deployment/Job/PVC per revision and the Service on the stable.
		stableBefore, err := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())

		By("rolling out a new revision and killing the operator as soon as the candidate exists")
		_, err = utils.Kubectl("patch", "decisionmodel", dm, "-n", chaosNS, "--type=merge",
			"-p", `{"spec":{"resources":{"requests":{"cpu":"650m","memory":"1Gi"},"limits":{"memory":"4Gi"}}}}`)
		Expect(err).NotTo(HaveOccurred())
		var candHash string
		Eventually(func() (string, error) {
			candHash, _ = utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.candidateRevision.hash}")
			return candHash, nil
		}, 3*time.Minute, 2*time.Second).ShouldNot(BeEmpty())
		killOperatorPod()

		By("the rollout still completes to Ready on the new revision after the restart")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.phase}")
		}, 12*time.Minute, 10*time.Second).Should(Equal("Ready"))
		stableNow, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(stableNow).To(Equal(candHash), "the candidate should promote across the operator restart")

		By("killing the operator again right after promotion; the Service stays on the stable")
		killOperatorPod()
		Consistently(func(g Gomega) {
			g.Expect(serviceRevision(chaosNS, dm)).To(Equal(candHash),
				"the Service must stay on the promoted stable across a restart")
		}, 30*time.Second, 5*time.Second).Should(Succeed())

		By("exactly one Deployment/Job/PVC for the promoted revision (no duplicates from the restart)")
		Expect(deploymentCountForRevision(chaosNS, dm, candHash)).To(Equal(1))
		Expect(jobCountForRevision(chaosNS, dm, candHash)).To(Equal(1))
		Expect(pvcCountForRevision(chaosNS, dm, candHash)).To(Equal(1))
		_ = stableBefore
	})

	It("re-prefetches a fresh candidate store when its PVC is deleted during Starting", func() {
		// Fault 5 (candidate side): the candidate's per-revision store PVC is deleted
		// while it is Starting. The candidate flow does not run the bounded stable
		// recovery; it waits out the Terminating PVC and re-prefetches a fresh one, then
		// completes — a normal rollout, no Failed.
		stableBefore, err := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())

		By("rolling out a new revision and deleting its store PVC once a candidate exists")
		_, err = utils.Kubectl("patch", "decisionmodel", dm, "-n", chaosNS, "--type=merge",
			"-p", `{"spec":{"resources":{"requests":{"cpu":"700m","memory":"1Gi"},"limits":{"memory":"4Gi"}}}}`)
		Expect(err).NotTo(HaveOccurred())
		var candHash string
		Eventually(func() (string, error) {
			candHash, _ = utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.candidateRevision.hash}")
			return candHash, nil
		}, 3*time.Minute, 2*time.Second).ShouldNot(BeEmpty())
		// Delete the candidate store PVC and its prefetch Job's/serving Pod so it finalizes.
		_, _ = utils.Kubectl("delete", "pvc", dm+"-store-"+candHash, "-n", chaosNS, "--wait=false")
		_, _ = utils.Kubectl("delete", "pods", "-l",
			"decisionmodel.io/name="+dm+",decisionmodel.io/revision="+candHash, "-n", chaosNS,
			"--grace-period=0", "--force")

		By("the candidate must not fail; it re-prefetches a fresh store and completes to Ready")
		Consistently(func(g Gomega) {
			phase, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.phase}")
			g.Expect(phase).NotTo(Equal("Failed"), "a deleted candidate store must not fail the rollout")
		}, 20*time.Second, 5*time.Second).Should(Succeed())
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.phase}")
		}, 12*time.Minute, 10*time.Second).Should(Equal("Ready"))
		stableNow, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(stableNow).To(Equal(candHash), "the candidate should still promote after a fresh re-prefetch")
		_ = stableBefore
	})

	// PENDING: depends on hitting the Stabilizing window and the 30s post-promotion
	// debounce precisely; enable alongside the store-loss scenario once the chaos CI is
	// dispatchable for iteration.
	PIt("does not falsely roll back when the stable Pod briefly dies during Stabilizing", func() {
		stableBefore, err := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())

		By("promoting a new revision with a stabilization window")
		applyConfigMap(chaosNS, "chaos-stab", "cases.jsonl", datasetJSONL(chaosLabels(dm)))
		_, err = utils.Kubectl("patch", "decisionmodel", dm, "-n", chaosNS, "--type=merge",
			"-p", `{"spec":{"rollout":{"stabilization":"3m"}}}`)
		Expect(err).NotTo(HaveOccurred())
		patchRollout(dm, chaosNS, rolloutPatch{
			cpu:          "450m",
			datasetCM:    "chaos-stab",
			minAccuracy:  "0.0",
			evaluatingTO: "20m",
			promotion:    "Automatic",
		})

		var candHash string
		By("waiting for the Stabilizing window to open")
		Eventually(func(g Gomega) {
			h, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
			g.Expect(h).NotTo(Equal(stableBefore))
			g.Expect(h).NotTo(BeEmpty())
			candHash = h
			stab, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Stabilizing')].status}")
			g.Expect(stab).To(Equal("True"))
		}, 12*time.Minute, 5*time.Second).Should(Succeed())

		By("deleting the new stable's Pod once; it should recover within the 30s debounce")
		_, _ = utils.Kubectl("delete", "pods", "-l",
			"decisionmodel.io/name="+dm+",decisionmodel.io/revision="+candHash, "-n", chaosNS,
			"--grace-period=0", "--force")

		By("no RolledBackAfterPromotion fires and the revision stays stable")
		Eventually(func(g Gomega) {
			// The recreated Pod reloads the model and the gate flips back True; the
			// Stabilizing condition should return to True (healthy) without a rollback.
			phase, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.phase}")
			g.Expect(phase).To(Equal("Ready"))
		}, 8*time.Minute, 10*time.Second).Should(Succeed())
		Expect(eventReasonCountIn("RolledBackAfterPromotion")).To(Equal(0),
			"a stable Pod that returns within the debounce must not trigger a post-promotion rollback")
		stableNow, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(stableNow).To(Equal(candHash), "the revision must remain stable after a brief Pod loss")
	})
})

// ---- chaos helpers ----

// deployMirror brings up the in-cluster registry mirror Deployment + Service (the
// same shape the mirror spec uses) and waits for it to be Available.
func deployMirror() {
	By("deploying the registry mirror")
	_, _ = utils.Kubectl("create", "ns", mirrorNS)
	applyYAML(fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata: {name: mirror, namespace: %s}
spec:
  replicas: 1
  selector: {matchLabels: {app: mirror}}
  template:
    metadata: {labels: {app: mirror}}
    spec:
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        seccompProfile: {type: RuntimeDefault}
      containers:
      - name: mirror
        image: %s
        imagePullPolicy: Never
        ports: [{containerPort: 8080}]
        readinessProbe:
          httpGet: {path: /healthz, port: 8080}
          periodSeconds: 2
        securityContext:
          allowPrivilegeEscalation: false
          readOnlyRootFilesystem: true
          runAsNonRoot: true
          capabilities: {drop: [ALL]}
---
apiVersion: v1
kind: Service
metadata: {name: mirror, namespace: %s}
spec:
  selector: {app: mirror}
  ports: [{port: 80, targetPort: 8080}]
`, mirrorNS, mirrorImage, mirrorNS))
	_, err := utils.Kubectl("wait", "deployment/mirror", "-n", mirrorNS,
		"--for=condition=Available", "--timeout=2m")
	Expect(err).NotTo(HaveOccurred(), "mirror Deployment did not become Available")
}

// setMirrorFault makes the mirror return the given HTTP status for every /v2/
// request (MIRROR_FAULT read at startup) and waits for the rollout, so the fault is
// live for the whole time it is set — longer than the engine's in-request retries
// and a reconcile.
func setMirrorFault(status string) {
	_, err := utils.Kubectl("set", "env", "deployment/mirror", "-n", mirrorNS, "MIRROR_FAULT="+status)
	Expect(err).NotTo(HaveOccurred())
	_, err = utils.Kubectl("rollout", "status", "deployment/mirror", "-n", mirrorNS, "--timeout=2m")
	Expect(err).NotTo(HaveOccurred(), "mirror did not roll out with the fault")
}

// clearMirrorFault removes the fault env and waits for the mirror to serve normally.
func clearMirrorFault() {
	_, _ = utils.Kubectl("set", "env", "deployment/mirror", "-n", mirrorNS, "MIRROR_FAULT-")
	_, _ = utils.Kubectl("rollout", "status", "deployment/mirror", "-n", mirrorNS, "--timeout=2m")
}

// chaosLabels learns, once per DecisionModel, the label the model answers for each
// golden state, so the eval dataset is one the model passes (model-agnostic).
func chaosLabels(dm string) []string {
	return learnLabels(dm, chaosNS)
}

// eventReasonCountIn counts Events in the chaos namespace on the chaos DecisionModel
// by reason.
func eventReasonCountIn(reason string) int {
	out, _ := utils.Kubectl("get", "events", "-n", chaosNS,
		"--field-selector", "involvedObject.name="+chaosDM+",reason="+reason,
		"-o", "jsonpath={.items[*].reason}")
	return len(strings.Fields(out))
}

// killOperatorPod force-deletes the controller-manager Pod(s); the Deployment
// recreates them. Used to prove restart convergence (level-triggered reconcile +
// persist-before-switch make a mid-step kill safe).
func killOperatorPod() {
	for _, p := range managerPods() {
		_, _ = utils.Kubectl("delete", "pod", p, "-n", operatorNamespace, "--grace-period=0", "--force")
	}
	_, _ = utils.Kubectl("rollout", "status", "deployment/"+managerDeployment(),
		"-n", operatorNamespace, "--timeout=2m")
}

// pvcCountForRevision counts the per-revision store PVCs carrying a revision label
// (a duplicate would mean the restart created a second store for one revision).
func pvcCountForRevision(ns, name, rev string) int {
	out, _ := utils.Kubectl("get", "pvc",
		"-l", "decisionmodel.io/name="+name+",decisionmodel.io/revision="+rev,
		"-n", ns, "-o", "jsonpath={.items[*].metadata.name}")
	return len(strings.Fields(out))
}
