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
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/maks3201/decision-model-operator/test/utils"
)

// rolloutNS isolates the manual-promotion scenario from the lifecycle and
// eval-gated suites so it can run with its own stable DecisionModel.
const rolloutNS = "dmo-e2e-rollout"

// This container covers promotion: Manual. It is labelled "nightly" because it
// needs its own stable + candidate model load, which would push the per-PR E2E
// run past its time budget; the per-PR workflow skips it (E2E_LABEL_FILTER
// =!nightly) and the nightly workflow runs it in full (and across the non-laya
// nightly models, since the behaviour is model-agnostic).
//
// The dataset-hold, kubectl-view and model@sha256:/"keeps serving" Event
// assertions live in the Eval-gated rollout container (eval_auth_test.go) where
// they reuse that suite's already-running stable DM and cost no extra load.
var _ = Describe("Rollout: manual promotion policy", Label("nightly"), Ordered, func() {
	const dm = "rollout-router"

	// rolloutVerified holds the label the model actually returns for each evalState,
	// learned live so the golden dataset the candidate passes is one the model gets
	// right regardless of the model under test (model-agnostic, like the eval suite).
	var rolloutVerified []string

	BeforeAll(func() {
		if testInstall == "helm" {
			Skip("helm job runs lifecycle specs only")
		}
		installAndDeploy()
		_, _ = utils.Kubectl("create", "ns", rolloutNS)

		By("bringing up a stable revision and learning the model's real labels")
		applyYAML(fmt.Sprintf(`apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata: {name: %s, namespace: %s}
spec:
  engine: ollaya
  model: %s
  device: %s
  replicas: 1
  resources: {requests: {cpu: 250m, memory: 1Gi}, limits: {memory: 4Gi}}
`, dm, rolloutNS, testModel, testDevice))
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(rolloutNS, "decisionmodel", dm, "{.status.phase}")
		}, 8*time.Minute, 5*time.Second).Should(Equal("Ready"))

		stop := make(chan struct{})
		local := portForward(dm, rolloutNS, 11435, stop)
		defer close(stop)
		rolloutVerified = make([]string, len(evalStates))
		for i, st := range evalStates {
			body, _ := json.Marshal(map[string]any{
				"model": testModel, "keep_alive": -1,
				"state": st, "questions": deptQuestion(),
			})
			var resp struct {
				Answers map[string]struct {
					Choice string `json:"choice"`
				} `json:"answers"`
			}
			Eventually(func(g Gomega) {
				out, err := utils.Run(exec.Command("curl", "-sS", "--max-time", "180", "-X", "POST",
					fmt.Sprintf("http://%s/v1/systemone", local),
					"-H", "Content-Type: application/json", "--data-binary", string(body)))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(json.Unmarshal([]byte(out), &resp)).To(Succeed(), "bad JSON: %s", out)
				g.Expect(resp.Answers).To(HaveKey("q"))
			}, 3*time.Minute, 5*time.Second).Should(Succeed())
			rolloutVerified[i] = resp.Answers["q"].Choice
		}
	})

	AfterAll(func() {
		_, _ = utils.Kubectl("delete", "ns", rolloutNS, "--ignore-not-found")
		undeploy()
	})

	AfterEach(func() { dumpDiag(rolloutNS, dm) })

	It("parks a passing candidate in AwaitingPromotion, promotes on the decisionmodel.io/promote annotation", func() {
		By("recording the stable revision and Service selector before the rollout")
		stableBefore, err := utils.KubectlJSONPath(rolloutNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stableBefore).NotTo(BeEmpty())

		By("creating a golden dataset the model passes and rolling out a Manual-promotion candidate")
		applyConfigMap(rolloutNS, "manual-dataset", "cases.jsonl", datasetJSONL(rolloutVerified))
		patchRollout(dm, rolloutNS, rolloutPatch{
			cpu:          "350m",
			datasetCM:    "manual-dataset",
			minAccuracy:  "0.5",
			evaluatingTO: "20m",
			promotion:    "Manual",
		})

		By("the candidate passes eval and parks in AwaitingPromotion (Service unchanged)")
		var candHash string
		Eventually(func(g Gomega) {
			phase, _ := utils.KubectlJSONPath(rolloutNS, "decisionmodel", dm, "{.status.phase}")
			g.Expect(phase).To(Equal("AwaitingPromotion"),
				"a passing Manual candidate should park in AwaitingPromotion")
			h, _ := utils.KubectlJSONPath(rolloutNS, "decisionmodel", dm, "{.status.candidateRevision.hash}")
			g.Expect(h).NotTo(BeEmpty())
			candHash = h
		}, 12*time.Minute, 10*time.Second).Should(Succeed())

		By("Promoted condition is False with reason PromotionPending and the Service still on stable")
		promotedReason, _ := utils.KubectlJSONPath(rolloutNS, "decisionmodel", dm,
			"{.status.conditions[?(@.type=='Promoted')].reason}")
		Expect(promotedReason).To(Equal("PromotionPending"),
			"Promoted reason should be PromotionPending while awaiting approval")
		Expect(serviceRevision(rolloutNS, dm)).To(Equal(stableBefore),
			"Service must stay on the stable revision while awaiting manual promotion")

		By("the parked state is stable (does not self-promote without the annotation)")
		Consistently(func() (string, error) {
			return utils.KubectlJSONPath(rolloutNS, "decisionmodel", dm, "{.status.phase}")
		}, 30*time.Second, 10*time.Second).Should(Equal("AwaitingPromotion"),
			"a Manual candidate must not promote itself")

		By("approving by setting decisionmodel.io/promote to the evaluation approvalId -> promoted")
		// With evaluation configured the approval value is status.evaluation.approvalId
		// (a bare revision hash is ignored once eval is set); read it from status so the
		// test tracks the contract rather than hard-coding a token. Fall back to the
		// candidate revision hash only if approvalId is not yet populated.
		approval, err := utils.KubectlJSONPath(rolloutNS, "decisionmodel", dm, "{.status.evaluation.approvalId}")
		Expect(err).NotTo(HaveOccurred())
		if approval == "" {
			approval = candHash
		}
		_, err = utils.Kubectl("annotate", "decisionmodel", dm, "-n", rolloutNS,
			"decisionmodel.io/promote="+approval, "--overwrite")
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(rolloutNS, "decisionmodel", dm, "{.status.phase}")
		}, 5*time.Minute, 5*time.Second).Should(Equal("Ready"),
			"candidate should promote once the promote annotation names its approvalId")
		stableNow, err := utils.KubectlJSONPath(rolloutNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stableNow).To(Equal(candHash), "the approved candidate should become the stable revision")
		Expect(serviceRevision(rolloutNS, dm)).To(Equal(candHash),
			"Service should route to the manually promoted revision")
	})
})

// ppNS isolates the post-promotion rollback scenario.
const ppNS = "dmo-e2e-postpromo"

// This container covers the post-promotion stabilization safety net: after a
// candidate is promoted, the previous revision's Deployment is kept (out of the
// Service) for the stabilization window; if the new stable then becomes unhealthy
// within the window, the operator switches traffic back to the previous revision,
// records status.failedRevision.reason=PostPromotionUnhealthy and emits a
// RolledBackAfterPromotion Event. Labelled "nightly" (own stable + candidate load).
//
// The fault is real and operator-detected: we unload the model from the promoted
// Pod's runtime (POST /api/decide keep_alive=0). The operator's periodic re-inspect
// (regate, ~60s) then finds /api/ps no longer serving the model, flips the Pod's
// readiness gate False, model-ready replicas drop below quorum, and the operator
// rolls back to the previous (still-running) revision within the window.
var _ = Describe("Rollout: post-promotion rollback", Label("nightly"), Ordered, func() {
	const dm = "postpromo-router"

	BeforeAll(func() {
		if testInstall == "helm" {
			Skip("helm job runs lifecycle specs only")
		}
		installAndDeploy()
		_, _ = utils.Kubectl("create", "ns", ppNS)

		By("bringing up a stable revision")
		applyYAML(fmt.Sprintf(`apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata: {name: %s, namespace: %s}
spec:
  engine: ollaya
  model: %s
  device: %s
  replicas: 1
  resources: {requests: {cpu: 250m, memory: 1Gi}, limits: {memory: 4Gi}}
`, dm, ppNS, testModel, testDevice))
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(ppNS, "decisionmodel", dm, "{.status.phase}")
		}, 8*time.Minute, 5*time.Second).Should(Equal("Ready"))
	})

	AfterAll(func() {
		_, _ = utils.Kubectl("delete", "ns", ppNS, "--ignore-not-found")
		undeploy()
	})

	AfterEach(func() { dumpDiag(ppNS, dm) })

	It("rolls back to the previous revision when the new stable becomes unhealthy in the window", func() {
		By("recording the stable revision before the rollout")
		prevStable, err := utils.KubectlJSONPath(ppNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(prevStable).NotTo(BeEmpty())

		By("promoting a new revision with a generous stabilization window (cpu bump)")
		// A long stabilization window keeps the previous revision alive while the fault
		// is injected and detected. No evaluation: promotion is automatic once the
		// candidate is model-ready.
		_, err = utils.Kubectl("patch", "decisionmodel", dm, "-n", ppNS, "--type=merge",
			"-p", `{"spec":{"resources":{"requests":{"cpu":"300m","memory":"1Gi"},"limits":{"memory":"4Gi"}},`+
				`"rollout":{"stabilization":"10m"}}}`)
		Expect(err).NotTo(HaveOccurred())

		var newStable string
		Eventually(func(g Gomega) {
			h, _ := utils.KubectlJSONPath(ppNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
			phase, _ := utils.KubectlJSONPath(ppNS, "decisionmodel", dm, "{.status.phase}")
			g.Expect(h).NotTo(Equal(prevStable), "a new revision should be promoted")
			g.Expect(h).NotTo(BeEmpty())
			g.Expect(phase).To(Equal("Ready"), "the new revision should promote to Ready")
			newStable = h
		}, 12*time.Minute, 10*time.Second).Should(Succeed())

		By("the previous revision is kept during the stabilization window (Stabilizing=True)")
		Eventually(func(g Gomega) {
			stabilizing, _ := utils.KubectlJSONPath(ppNS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Stabilizing')].status}")
			prev, _ := utils.KubectlJSONPath(ppNS, "decisionmodel", dm, "{.status.previousRevision.hash}")
			g.Expect(stabilizing).To(Equal("True"), "Stabilizing condition should be True in the window")
			g.Expect(prev).To(Equal(prevStable), "previousRevision should be the demoted stable, kept for rollback")
		}, 2*time.Minute, 5*time.Second).Should(Succeed())
		Expect(serviceRevision(ppNS, dm)).To(Equal(newStable), "Service should route to the new revision after promotion")

		By("making the new stable unhealthy: hold its serving Pods down so model-ready stays below quorum")
		// A single Pod deletion self-heals (the Deployment recreates a Pod that reloads
		// the model within a reconcile), so to create the SUSTAINED shortfall the operator
		// treats as unhealthy we keep the new stable's Pods deleted for longer than the
		// post-promotion debounce. This models a new model that cannot stay up (crash loop
		// / repeated eviction) — a real failure the stabilization window exists to catch.
		// The previous revision's Pods (a different revision label) are left untouched.
		stopKill := make(chan struct{})
		var killOnce sync.Once
		stopKilling := func() { killOnce.Do(func() { close(stopKill) }) }
		go holdRevisionPodsDown(ppNS, dm, newStable, stopKill)
		defer stopKilling()

		By("the operator detects the unhealthy new stable and rolls back to the previous revision")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(ppNS, "decisionmodel", dm, "{.status.phase}")
		}, 10*time.Minute, 10*time.Second).Should(Equal("RolledBack"),
			"the operator should roll back after the new stable stays unhealthy")
		stopKilling()

		By("status.failedRevision.reason is PostPromotionUnhealthy and the failed revision is the new one")
		reason, err := utils.KubectlJSONPath(ppNS, "decisionmodel", dm, "{.status.failedRevision.reason}")
		Expect(err).NotTo(HaveOccurred())
		Expect(reason).To(Equal("PostPromotionUnhealthy"))
		failed, _ := utils.KubectlJSONPath(ppNS, "decisionmodel", dm, "{.status.failedRevision.hash}")
		Expect(failed).To(Equal(newStable), "the failed revision should be the unhealthy new stable")

		By("the Service is back on the previous revision and it is the stable again")
		Eventually(func(g Gomega) {
			stableNow, _ := utils.KubectlJSONPath(ppNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
			g.Expect(stableNow).To(Equal(prevStable), "the previous revision should be restored as stable")
			g.Expect(serviceRevision(ppNS, dm)).To(Equal(prevStable),
				"Service should route back to the previous revision")
		}, 3*time.Minute, 5*time.Second).Should(Succeed())

		By("a RolledBackAfterPromotion Event was emitted")
		msg, err := utils.Kubectl("get", "events", "-n", ppNS,
			"--field-selector", "reason=RolledBackAfterPromotion",
			"-o", "jsonpath={.items[-1:].message}")
		Expect(err).NotTo(HaveOccurred())
		Expect(msg).NotTo(BeEmpty(), "expected a RolledBackAfterPromotion Event")
		_, _ = fmt.Fprintf(GinkgoWriter, "RolledBackAfterPromotion: %s\n", msg)
	})
})

// ---- helpers shared by the rollout specs (this file + the eval-gated container) ----

// holdRevisionPodsDown repeatedly force-deletes the Pods of a specific revision
// until stop is closed, keeping that revision's model-ready replicas below quorum
// long enough for the operator's post-promotion debounce to elapse (a single
// deletion self-heals when the recreated Pod reloads the model within a reconcile).
// Scoped to one revision label so the previous (kept) revision is untouched.
func holdRevisionPodsDown(ns, name, revision string, stop <-chan struct{}) {
	defer GinkgoRecover()
	for {
		select {
		case <-stop:
			return
		default:
		}
		out, err := utils.Kubectl("get", "pods",
			"-l", "decisionmodel.io/name="+name+",decisionmodel.io/revision="+revision,
			"-n", ns, "-o", "jsonpath={.items[*].metadata.name}")
		if err == nil {
			for _, p := range strings.Fields(out) {
				_, _ = utils.Kubectl("delete", "pod", p, "-n", ns, "--grace-period=0", "--force")
			}
		}
		select {
		case <-stop:
			return
		case <-time.After(5 * time.Second):
		}
	}
}

// rolloutPatch bundles the fields of a merge patch that forces a new eval-gated
// candidate revision. cpu bumps the request (the revision hash includes resources,
// so a change forces a new revision); datasetCM/minAccuracy set the eval gate;
// evaluatingTO sets a generous Evaluating timeout so a dataset hold does not
// fail-close during the test; promotion, when set, selects the promotion policy.
type rolloutPatch struct {
	cpu          string
	datasetCM    string
	minAccuracy  string
	minMacroF1   string
	evaluatingTO string
	promotion    string
}

// patchRollout applies rolloutPatch as a merge patch to the DecisionModel.
func patchRollout(name, ns string, p rolloutPatch) {
	evaluation := map[string]any{
		"datasetRef":  map[string]any{"configMapRef": map[string]any{"name": p.datasetCM, "key": "cases.jsonl"}},
		"minAccuracy": p.minAccuracy,
		// Always send minMacroF1 so a merge patch clears a previously-set gate when the
		// caller leaves it empty (merge patches do not drop omitted fields). nil marshals
		// to JSON null, which removes the field; a value sets the gate.
		"minMacroF1": nil,
	}
	if p.minMacroF1 != "" {
		evaluation["minMacroF1"] = p.minMacroF1
	}
	rollout := map[string]any{
		"evaluation": evaluation,
	}
	if p.evaluatingTO != "" {
		rollout["timeouts"] = map[string]any{"evaluating": p.evaluatingTO}
	}
	if p.promotion != "" {
		rollout["promotion"] = p.promotion
	}
	patch := map[string]any{
		"spec": map[string]any{
			"resources": map[string]any{
				"requests": map[string]any{"cpu": p.cpu, "memory": "1Gi"},
				"limits":   map[string]any{"memory": "4Gi"},
			},
			"rollout": rollout,
		},
	}
	raw, err := json.Marshal(patch)
	Expect(err).NotTo(HaveOccurred())
	_, err = utils.Kubectl("patch", "decisionmodel", name, "-n", ns, "--type=merge", "-p", string(raw))
	Expect(err).NotTo(HaveOccurred())
}

// serviceRevision returns the revision the DM's Service currently selects.
func serviceRevision(ns, name string) string {
	rev, _ := utils.KubectlJSONPath(ns, "svc", name, "{.spec.selector.decisionmodel\\.io/revision}")
	return rev
}

// dmColumns runs `kubectl get dm <name>` in the default (non-wide) table form and
// returns the printer columns as a map keyed by the uppercased header name
// (ACTIVE, CANDIDATE, ACCURACY, PHASE, REASON, AGE). It parses the two-line table
// output (header row + one data row) by header column position so an empty cell or
// a "<none>" value is handled by position rather than by Fields() (which would
// drop empty columns and misalign). This tests exactly what a human sees,
// including the CRD's additionalPrinterColumns ordering.
func dmColumns(ns, name string) (map[string]string, error) {
	out, err := utils.Kubectl("get", "dm", name, "-n", ns)
	if err != nil {
		return nil, err
	}
	lines := utils.SplitLines(strings.TrimRight(out, "\n"))
	if len(lines) < 2 {
		return nil, fmt.Errorf("expected a header and a data row, got %q", out)
	}
	headers := strings.Fields(lines[0])
	cols := make(map[string]string, len(headers))
	data := lines[1]
	for i, h := range headers {
		start := strings.Index(lines[0], h)
		end := len(data)
		if i+1 < len(headers) {
			if next := strings.Index(lines[0], headers[i+1]); next >= 0 {
				end = next
			}
		}
		if start < 0 || start > len(data) {
			cols[strings.ToUpper(h)] = ""
			continue
		}
		if end > len(data) {
			end = len(data)
		}
		cols[strings.ToUpper(h)] = strings.TrimSpace(data[start:end])
	}
	return cols, nil
}
