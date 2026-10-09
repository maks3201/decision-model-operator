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
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/maks3201/decision-model-operator/test/utils"
)

// macroF1NS isolates the macro-F1 gate scenario from the other eval specs.
const macroF1NS = "dmo-e2e-macrof1"

// macroF1DM is the DecisionModel this container drives.
const macroF1DM = "macrof1-router"

// f1Pool is a deliberately class-imbalanced pool of states. They are all phrased
// around billing/payments so laya:en predicts the SAME choice for (almost) all of
// them; the actual predicted label is MEASURED at runtime (never assumed) and the
// golden dataset is built from those measurements. The imbalance is what makes
// accuracy and macro-F1 disagree: a golden set dominated by one class, where the
// candidate is right on the majority and wrong on a lone minority case, scores high
// accuracy (the majority carries it) but low macro-F1 (the minority class gets
// F1 = 0, and macro-F1 is the unweighted mean over classes).
var f1Pool = []string{
	"I was charged twice for my subscription this month, please refund the extra charge.",
	"My invoice shows a payment I did not authorize, I want my money back.",
	"You billed my card after I cancelled; please reverse the charge.",
	"There is a duplicate charge on my statement for the annual plan.",
	"I need a refund for an overcharge on my latest invoice.",
	"My credit card was charged the wrong amount for this month's bill.",
}

// f1Question returns the questions object used throughout this container: the same
// choice question "q" with the shared department criteria, so the live predictions
// and the golden dataset score like with like.
func f1Question() map[string]any {
	return map[string]any{
		"q": map[string]any{"type": "choice", "criteria": deptCriteria},
	}
}

var _ = Describe("Eval-gated macro-F1 gate", Label("eval"), Ordered, func() {
	const dm = macroF1DM

	// Learned once in BeforeAll from the live model.
	var (
		// minorityLabel is a valid criterion key the model did not predict, used as
		// the lone minority expected label (recall 0 -> drags macro-F1 down).
		minorityLabel string
		// imbalanced is the golden JSONL built from the measured predictions with one
		// case flipped to the minority class.
		imbalanced string
		// minorityIdx is the single case whose expected label is the minority class.
		minorityIdx int
	)

	BeforeAll(func() {
		if testInstall == "helm" {
			Skip("helm job runs lifecycle specs only")
		}
		installAndDeploy()
		_, _ = utils.Kubectl("create", "ns", macroF1NS)

		By("bringing up a stable revision to learn the model's real labels")
		applyYAML(fmt.Sprintf(`apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata: {name: %s, namespace: %s}
spec:
  engine: ollaya
  model: %s
  device: %s
  replicas: 1
  resources: {requests: {cpu: 250m, memory: 1Gi}, limits: {memory: 4Gi}}
`, dm, macroF1NS, testModel, testDevice))
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(macroF1NS, "decisionmodel", dm, "{.status.phase}")
		}, 8*time.Minute, 5*time.Second).Should(Equal("Ready"))

		By("measuring the model's prediction for every pool state (nothing is assumed)")
		stop := make(chan struct{})
		local := portForward(dm, macroF1NS, 11435, stop)
		defer close(stop)

		preds := make([]string, len(f1Pool))
		for i, st := range f1Pool {
			preds[i] = measureChoice(local, st)
			_, _ = fmt.Fprintf(GinkgoWriter, "pool case %d -> %q\n", i, preds[i])
		}

		By("building the imbalanced golden set from the measured predictions")
		var labels []string
		labels, minorityLabel, minorityIdx = buildImbalanced(preds)
		_, _ = fmt.Fprintf(GinkgoWriter,
			"labels=%v minority=%q minorityIdx=%d\n", labels, minorityLabel, minorityIdx)
		Expect(labels).NotTo(BeEmpty(),
			"could not build an imbalanced golden set from the pool (predictions=%v)", preds)
		Expect(minorityLabel).NotTo(BeEmpty())
		imbalanced = poolDatasetJSONL(labels)

		// Sanity-check the construction locally: accuracy is high (only the single
		// minority case is "wrong" vs the measured prediction), macro-F1 is low (the
		// minority class has recall 0). This mirrors what the operator will score, so
		// the thresholds below are chosen to straddle them.
		correct := 0
		for i := range preds {
			if preds[i] == labels[i] {
				correct++
			}
		}
		acc := float64(correct) / float64(len(preds))
		_, _ = fmt.Fprintf(GinkgoWriter, "constructed golden set: accuracy=%.3f over %d cases\n", acc, len(preds))
		Expect(acc).To(BeNumerically(">=", 0.75),
			"the imbalanced set must still pass minAccuracy=0.70 (got accuracy %.3f)", acc)
	})

	AfterAll(func() {
		_, _ = utils.Kubectl("delete", "ns", macroF1NS, "--ignore-not-found")
		undeploy()
	})

	AfterEach(func() { dumpDiag(macroF1NS, dm) })

	It("rejects a candidate that passes minAccuracy but fails minMacroF1 (stable keeps serving)", func() {
		By("recording the stable revision before the failing rollout")
		stableBefore, err := utils.KubectlJSONPath(macroF1NS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stableBefore).NotTo(BeEmpty())
		Expect(serviceRevision(macroF1NS, dm)).To(Equal(stableBefore),
			"Service should route to the stable revision before the rollout")

		By("publishing the imbalanced golden set")
		applyConfigMap(macroF1NS, "golden-imbalanced", "cases.jsonl", imbalanced)

		By("rolling out an eval-gated candidate with minAccuracy=0.70 AND minMacroF1=0.90")
		// Accuracy clears 0.70 (the majority class carries it); macro-F1 is well
		// below 0.90 because the lone minority class scores F1=0. So the ONLY gate
		// that can reject this candidate is minMacroF1 — which is the point of the test.
		patchRollout(dm, macroF1NS, rolloutPatch{
			cpu:         "300m",
			datasetCM:   "golden-imbalanced",
			minAccuracy: "0.70",
			minMacroF1:  "0.90",
		})

		By("capturing the candidate revision hash (it must never be promoted)")
		var failRev string
		Eventually(func(g Gomega) {
			h, _ := utils.KubectlJSONPath(macroF1NS, "decisionmodel", dm, "{.status.candidateRevision.hash}")
			g.Expect(h).NotTo(BeEmpty())
			failRev = h
			g.Expect(failRev).NotTo(Equal(stableBefore), "candidate must be a new revision")
		}, 2*time.Minute, 5*time.Second).Should(Succeed())

		By("phase reaches RolledBack")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(macroF1NS, "decisionmodel", dm, "{.status.phase}")
		}, 12*time.Minute, 5*time.Second).Should(Equal("RolledBack"),
			"a macro-F1 failure must roll back to the stable")

		By("the rollback is specifically EvaluationFailed with failedRevision set")
		Eventually(func(g Gomega) {
			reason, _ := utils.KubectlJSONPath(macroF1NS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Degraded')].reason}")
			fr, _ := utils.KubectlJSONPath(macroF1NS, "decisionmodel", dm, "{.status.failedRevision.hash}")
			g.Expect(fr).NotTo(BeEmpty(), "failedRevision should be set")
			g.Expect(reason).To(Equal("EvaluationFailed"),
				"expected an EvaluationFailed rollback (got Degraded reason %q)", reason)
		}, 12*time.Minute, 10*time.Second).Should(Succeed())

		By("status.evaluation records the macro-F1 comparison and a populated questions breakdown")
		// The macro-F1 gate must be observable on the CR: the recorded gate reason names
		// the minMacroF1 comparison, macroF1 is recorded, and the per-question breakdown
		// (status.evaluation.questions) is populated (the choice question that fed macro-F1).
		Eventually(func(g Gomega) {
			reason, _ := utils.KubectlJSONPath(macroF1NS, "decisionmodel", dm, "{.status.evaluation.reason}")
			g.Expect(reason).To(ContainSubstring("minMacroF1"),
				"status.evaluation.reason should name the macro-F1 comparison (got %q)", reason)
			g.Expect(reason).To(ContainSubstring("macroF1"),
				"status.evaluation.reason should state the measured macroF1 (got %q)", reason)
			result, _ := utils.KubectlJSONPath(macroF1NS, "decisionmodel", dm, "{.status.evaluation.result}")
			g.Expect(result).To(Equal("Failed"), "status.evaluation.result should be Failed")
			mf1, _ := utils.KubectlJSONPath(macroF1NS, "decisionmodel", dm, "{.status.evaluation.macroF1}")
			g.Expect(mf1).NotTo(BeEmpty(), "status.evaluation.macroF1 should be recorded")
			qn, _ := utils.KubectlJSONPath(macroF1NS, "decisionmodel", dm,
				"{.status.evaluation.questions[0].id}")
			g.Expect(qn).NotTo(BeEmpty(),
				"status.evaluation.questions should be populated with the classifiable question")
		}, 2*time.Minute, 5*time.Second).Should(Succeed())

		By("the failing candidate was never promoted and the stable is unchanged")
		stableNow, err := utils.KubectlJSONPath(macroF1NS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stableNow).To(Equal(stableBefore), "stable revision changed across a macro-F1 rejection")
		Expect(stableNow).NotTo(Equal(failRev), "the macro-F1 failing candidate must not be promoted")
		Expect(serviceRevision(macroF1NS, dm)).To(Equal(stableBefore),
			"Service must still route to the stable revision after the rejection")

		By("the EvaluationFailed Event names the macro-F1 comparison and says the stable keeps serving")
		msg, err := utils.Kubectl("get", "events", "-n", macroF1NS,
			"--field-selector", "reason=EvaluationFailed",
			"-o", "jsonpath={.items[-1:].message}")
		Expect(err).NotTo(HaveOccurred())
		Expect(msg).To(ContainSubstring("minMacroF1"),
			"EvaluationFailed Event should name the macro-F1 comparison (got %q)", msg)
		Expect(msg).To(ContainSubstring("keeps serving"),
			"EvaluationFailed Event should state the stable keeps serving (got %q)", msg)

		By("the failed candidate's store PVC was garbage-collected")
		Eventually(func(g Gomega) {
			g.Expect(storePVCRevisionsIn(macroF1NS, dm)).NotTo(ContainElement(failRev),
				"failed revision's store PVC should be deleted")
		}, 3*time.Minute, 5*time.Second).Should(Succeed())
	})

	It("promotes the SAME imbalanced set without minMacroF1 (proves the gate is what rejected it)", func() {
		// Same dataset, same minAccuracy, only minMacroF1 dropped. If the candidate now
		// promotes, the macro-F1 gate — and nothing else — is what rejected the previous
		// rollout. A fresh revision is forced (new cpu) so this is a real new candidate.
		By("recording the stable revision before the passing rollout")
		stableBefore, err := utils.KubectlJSONPath(macroF1NS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stableBefore).NotTo(BeEmpty())

		By("waiting until only the stable Pod remains (free memory before the next cold load)")
		Eventually(func(g Gomega) {
			out, err := utils.Kubectl("get", "pods", "-l", "decisionmodel.io/name="+dm,
				"-n", macroF1NS, "--field-selector=status.phase=Running",
				"-o", "jsonpath={.items[*].metadata.name}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.Fields(out)).To(HaveLen(1), "expected exactly one running serving Pod, got %q", out)
		}, 5*time.Minute, 10*time.Second).Should(Succeed())

		By("rolling out the same dataset with only minAccuracy=0.70 (no minMacroF1)")
		patchRollout(dm, macroF1NS, rolloutPatch{
			cpu:         "320m",
			datasetCM:   "golden-imbalanced",
			minAccuracy: "0.70",
		})

		var candHash string
		Eventually(func(g Gomega) {
			h, _ := utils.KubectlJSONPath(macroF1NS, "decisionmodel", dm, "{.status.candidateRevision.hash}")
			g.Expect(h).NotTo(BeEmpty())
			candHash = h
			g.Expect(candHash).NotTo(Equal(stableBefore), "candidate must be a new revision")
		}, 2*time.Minute, 5*time.Second).Should(Succeed())

		By("phase reaches Ready (the candidate is promoted)")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(macroF1NS, "decisionmodel", dm, "{.status.phase}")
		}, 12*time.Minute, 5*time.Second).Should(Equal("Ready"),
			"without a macro-F1 gate the same set must promote on accuracy alone")

		By("the candidate became the stable revision and the Service follows it")
		stableNow, err := utils.KubectlJSONPath(macroF1NS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stableNow).To(Equal(candHash), "the passing candidate should now be the stable revision")
		Expect(serviceRevision(macroF1NS, dm)).To(Equal(candHash),
			"Service should route to the promoted revision")

		By("status.evaluation records Passed and the measured accuracy")
		result, _ := utils.KubectlJSONPath(macroF1NS, "decisionmodel", dm, "{.status.evaluation.result}")
		Expect(result).To(Equal("Passed"), "status.evaluation.result should be Passed")
		acc, _ := utils.KubectlJSONPath(macroF1NS, "decisionmodel", dm, "{.status.evaluation.accuracy}")
		Expect(acc).NotTo(BeEmpty(), "status.evaluation.accuracy should be recorded")
		_, _ = fmt.Fprintf(GinkgoWriter, "promoted on accuracy=%s (no macro-F1 gate)\n", acc)
	})
})

// ---- macro-F1 helpers ----

// measureChoice posts a single choice question "q" (shared criteria) for one state
// through the port-forwarded Service and returns the label the model actually chose.
// It retries until the model answers with valid JSON carrying the "q" answer, so a
// cold model load does not flake the measurement.
func measureChoice(local, state string) string {
	body, _ := json.Marshal(map[string]any{
		"model": testModel, "keep_alive": -1,
		"state": state, "questions": f1Question(),
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
	return resp.Answers["q"].Choice
}

// buildImbalanced inspects the MEASURED pool predictions and returns a per-case
// expected-label vector that is high-accuracy but low-macro-F1, robustly for ANY
// model (it does not assume one predicted class already dominates). Construction:
//
//   - label every case with its OWN measured prediction, so those cases are correct;
//   - flip exactly ONE case's expected label to a "minority" class the model did not
//     predict for any case. That one case becomes a miss (accuracy = (n-1)/n), and
//     the minority class has one expected instance with zero predictions -> recall 0,
//     F1 0, which drags macro-F1 (the unweighted mean over classes) well below the
//     per-class scores while accuracy stays high.
//
// This needs only n >= 4 pool cases (so (n-1)/n >= 0.75) and at least one of the
// four criterion keys left unpredicted to use as the minority class -- both hold for
// the billing-heavy pool across models. It returns the full expected-label vector,
// the minority label and the flipped index; majority is "" only if no unpredicted
// criterion key exists (the caller fails with a clear message), which would mean the
// model somehow used all four classes on a 6-case billing pool.
func buildImbalanced(preds []string) (labels []string, minority string, minorityIdx int) {
	// Start from the model's own predictions: every case correct.
	labels = append([]string(nil), preds...)

	counts := map[string]int{}
	for _, p := range preds {
		counts[p]++
	}
	// Minority class: a valid criterion key the model did NOT predict (so it has
	// recall 0 once we assign one expected case to it). Deterministic order.
	order := []string{"technical", "sales", "other", "billing"}
	for _, cand := range order {
		if counts[cand] == 0 {
			minority = cand
			break
		}
	}
	if minority == "" || len(preds) < 4 {
		// No unpredicted class to use (all four criteria appeared), or too few cases
		// to keep accuracy >= 0.75. Neither is expected for the billing pool; the
		// caller fails with the predictions in the message.
		return nil, "", 0
	}

	// Flip the LAST case deterministically. Its expected label becomes the minority
	// class while its prediction is whatever the model said (!= minority, since the
	// minority class is unpredicted), so it is a guaranteed miss for the minority
	// class. Using a fixed index keeps the construction stable across runs.
	minorityIdx = len(preds) - 1
	labels[minorityIdx] = minority
	return labels, minority, minorityIdx
}

// poolDatasetJSONL builds a JSONL golden dataset over f1Pool: one choice question
// "q" per state (shared criteria) with the given expected label per case.
func poolDatasetJSONL(labels []string) string {
	var b bytes.Buffer
	for i, st := range f1Pool {
		state, _ := json.Marshal(st)
		q, _ := json.Marshal(f1Question())
		exp, _ := json.Marshal(map[string]string{"q": labels[i]})
		fmt.Fprintf(&b, `{"state":%s,"questions":%s,"expected":%s}`+"\n", state, q, exp)
	}
	return b.String()
}

// storePVCRevisionsIn is storePVCRevisions for an arbitrary namespace/DM (the
// package-level storePVCRevisions is pinned to the eval suite's namespace/DM).
func storePVCRevisionsIn(ns, dm string) []string {
	out, err := utils.Kubectl("get", "pvc",
		"-l", "decisionmodel.io/name="+dm, "-n", ns,
		"-o", "jsonpath={range .items[*]}{.metadata.labels.decisionmodel\\.io/revision}"+
			"{\" \"}{.metadata.deletionTimestamp}{\"\\n\"}{end}")
	Expect(err).NotTo(HaveOccurred())
	var revs []string
	for _, line := range utils.SplitLines(strings.TrimSpace(out)) {
		fields := strings.Fields(line)
		if len(fields) == 0 || len(fields) >= 2 {
			continue // blank, or has a deletionTimestamp (Terminating) -> treat as gone
		}
		revs = append(revs, fields[0])
	}
	return revs
}
