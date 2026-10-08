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

// This container injects registry, runtime and cluster faults at specific reconcile
// steps and asserts the documented recovery. It is nightly-only (slow: it drives
// several candidate model loads) and runs through the in-cluster mirror so the
// registry can be broken. Faults are held long enough to outlast the engine's
// in-request retries (3) and at least one reconcile, otherwise nothing is observable.
//
// Scenarios (one DecisionModel reused where possible to bound node memory — every
// laya:en Pod holds the model, so the suite runs them sequentially):
//  1. Registry 500 during Resolving  -> stable keeps serving, Resolved=False/ResolveFailed
//     (transient, no Degraded, no Event); fault cleared -> the candidate proceeds.
//  2. Registry 404 for the tag       -> Resolved=False/ModelNotFound, stable keeps serving.
//  3. Runtime fault during Evaluating (model unloaded from the candidate Pod) ->
//     evaluation holds and retries, no false reject; model back -> promotes.
//  4. Stable store PVC deleted while Ready -> Degraded=StoreLost, bounded recovery.
//  5. Stable serving Pod deleted during Stabilizing -> no false rollback when it
//     returns within the debounce.
var _ = Describe("Chaos: faults between critical steps", Label("nightly", "chaos"), Ordered, func() {
	const dm = "chaos-router"

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
		Expect(eventReasonCountIn(chaosNS, dm, "CandidateRejected")).To(Equal(0),
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
		Expect(eventReasonCountIn(chaosNS, dm, "CandidateRejected")).To(BeNumerically(">=", 1),
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

		By("waiting until the candidate is Evaluating, then disrupting its serving Pod")
		var candHash string
		Eventually(func(g Gomega) {
			phase, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.phase}")
			h, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.candidateRevision.hash}")
			g.Expect(h).NotTo(BeEmpty())
			candHash = h
			g.Expect(phase).To(BeElementOf("Evaluating", "Ready"))
		}, 10*time.Minute, 5*time.Second).Should(Succeed())

		// Disrupt the candidate Pod for ~45s (longer than one eval attempt + a reconcile)
		// while asserting the candidate does not roll back / fail or get a false reject.
		stop := make(chan struct{})
		go holdRevisionPodsDown(chaosNS, dm, candHash, stop)
		Consistently(func(g Gomega) {
			phase, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.phase}")
			g.Expect(phase).NotTo(BeElementOf("Failed", "RolledBack"),
				"a runtime transport fault during eval must not false-reject the candidate")
			ev, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Evaluated')].reason}")
			g.Expect(ev).NotTo(Equal("EvaluationFailed"),
				"a transport blip must not be scored as an evaluation failure")
		}, 45*time.Second, 5*time.Second).Should(Succeed())
		close(stop)

		By("with the runtime healthy again the candidate completes evaluation and promotes")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.phase}")
		}, 10*time.Minute, 5*time.Second).Should(Equal("Ready"))
		stableNow, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(stableNow).To(Equal(candHash), "the candidate should promote once the runtime recovers")
		_ = stableBefore
	})

	It("recovers a Degraded=StoreLost when the stable store PVC is deleted while Ready", func() {
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

		By("Degraded becomes True with a store reason (StoreTerminating or StoreLost)")
		Eventually(func(g Gomega) {
			deg, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Degraded')].status}")
			reason, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Degraded')].reason}")
			g.Expect(deg).To(Equal("True"))
			g.Expect(reason).To(BeElementOf("StoreLost", "StoreTerminating", "StorePrefetchFailed"),
				"a deleted stable store should surface a store Degraded reason (got %q)", reason)
		}, 4*time.Minute, 5*time.Second).Should(Succeed())

		By("the operator recreates the store and the DecisionModel returns to Ready")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.phase}")
		}, 10*time.Minute, 10*time.Second).Should(Equal("Ready"),
			"bounded store recovery should re-prefetch and serve again")
		stableNow, _ := utils.KubectlJSONPath(chaosNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(stableNow).To(Equal(stable), "recovery keeps the same revision (re-prefetch, not a new rollout)")
	})

	It("does not falsely roll back when the stable Pod briefly dies during Stabilizing", func() {
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
		Expect(eventReasonCountIn(chaosNS, dm, "RolledBackAfterPromotion")).To(Equal(0),
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

// eventReasonCountIn counts Events in a namespace on a DecisionModel by reason.
func eventReasonCountIn(ns, name, reason string) int {
	out, _ := utils.Kubectl("get", "events", "-n", ns,
		"--field-selector", "involvedObject.name="+name+",reason="+reason,
		"-o", "jsonpath={.items[*].reason}")
	return len(strings.Fields(out))
}
