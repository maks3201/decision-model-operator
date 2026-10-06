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

// authNS / evalNS keep these scenarios isolated from the lifecycle suite.
const authNS = "dmo-e2e-auth"
const evalNS = "dmo-e2e-eval"

// evalDM is the DecisionModel name used throughout the Eval-gated rollout container.
// It is package-scoped so the store-PVC helpers can reference it without taking it as
// an always-constant parameter (unparam).
const evalDM = "eval-router"

// deptCriteria is the choice question sent to laya:en. Kept identical between the
// live pre-check and the golden dataset so scoring compares like with like.
var deptCriteria = map[string]string{
	"billing":   "billing, payments, charges, refunds, invoices",
	"technical": "technical bugs, errors, crashes, outages",
	"sales":     "pricing, plans, upgrades, purchasing",
	"other":     "anything else",
}

// evalStates are the golden cases. Their expected labels are learned from the
// live model at runtime (task: verify expected labels against laya:en first).
var evalStates = []string{
	"I was charged twice for my subscription this month, please refund the extra charge.",
	"My invoice shows a payment I did not authorize.",
	"The app crashes every time I open the dashboard.",
	"I keep getting a 500 error when saving settings.",
	"Do you offer an annual plan with a discount?",
}

// deptQuestion returns the questions object for a single choice question "q".
func deptQuestion() map[string]any {
	return map[string]any{
		"q": map[string]any{"type": "choice", "criteria": deptCriteria},
	}
}

var _ = Describe("API-key auth", Ordered, func() {
	const dm = "auth-router"
	const secretName = "dm-key"
	const badSecretName = "dm-key-unlabelled"
	const apiKeyValue = "s3cr3t-e2e-key"

	BeforeAll(func() {
		if testInstall == "helm" {
			Skip("helm job runs lifecycle specs only")
		}
		installAndDeploy()
		_, _ = utils.Kubectl("create", "ns", authNS)

		By("creating a labelled API-key Secret")
		applyYAML(fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: %s
  namespace: %s
  labels:
    decisionmodel.io/api-key: "true"
stringData:
  token: %s
`, secretName, authNS, apiKeyValue))
	})

	AfterAll(func() {
		_, _ = utils.Kubectl("delete", "ns", authNS, "--ignore-not-found")
		undeploy()
	})

	AfterEach(func() { dumpDiag(authNS, dm) })

	It("requires the API key: 401 without, 200 with; DM Ready", func() {
		By("applying a DecisionModel with auth.apiKeySecretRef")
		applyYAML(fmt.Sprintf(`apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata: {name: %s, namespace: %s}
spec:
  engine: ollaya
  model: %s
  device: %s
  replicas: 1
  resources: {requests: {cpu: 250m, memory: 1Gi}, limits: {memory: 4Gi}}
  auth:
    apiKeySecretRef: {name: %s, key: token}
`, dm, authNS, testModel, testDevice, secretName))

		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(authNS, "decisionmodel", dm, "{.status.phase}")
		}, 8*time.Minute, 5*time.Second).Should(Equal("Ready"))

		stop := make(chan struct{})
		local := portForward(dm, authNS, 11435, stop)
		defer close(stop)

		body, _ := json.Marshal(map[string]any{
			"model": testModel, "keep_alive": -1,
			"state": evalStates[0], "questions": deptQuestion(),
		})

		By("no Authorization header -> 401")
		Eventually(func(g Gomega) {
			code, err := utils.Run(exec.Command("curl", "-sS", "-o", "/dev/null", "-w", "%{http_code}",
				"--max-time", "180", "-X", "POST", fmt.Sprintf("http://%s/v1/systemone", local),
				"-H", "Content-Type: application/json", "--data-binary", string(body)))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(code).To(Equal("401"), "expected 401 without the API key")
		}, 3*time.Minute, 5*time.Second).Should(Succeed())

		By("with Authorization: Bearer <key> -> 200")
		code, err := utils.Run(exec.Command("curl", "-sS", "-o", "/dev/null", "-w", "%{http_code}",
			"--max-time", "180", "-X", "POST", fmt.Sprintf("http://%s/v1/systemone", local),
			"-H", "Content-Type: application/json",
			"-H", "Authorization: Bearer "+apiKeyValue, "--data-binary", string(body)))
		Expect(err).NotTo(HaveOccurred())
		Expect(code).To(Equal("200"), "expected 200 with the API key")
	})

	It("rotates the API key in place: Pods roll, DM back to Ready, new key live", func() {
		// Runs after the first It, which left auth-router Ready with apiKeyValue.:
		// the operator does NOT watch Secrets; it reads the Secret each reconcile and records
		// decisionmodel.io/apikey-checksum on the serving Deployment, mirroring it onto the Pod
		// template (and rolling) only when the value actually changes. Pickup is on the next
		// periodic reconcile (~60 s), not instant.
		const newKey = "rotated-e2e-key-v2"

		By("recording the stable revision, its apikey-checksum and the current Pod before rotation")
		rev, err := utils.KubectlJSONPath(authNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(rev).NotTo(BeEmpty(), "stable revision hash not set")
		dep := dm + "-" + rev
		checksumBefore, err := utils.KubectlJSONPath(authNS, "deploy", dep,
			"{.metadata.annotations.decisionmodel\\.io/apikey-checksum}")
		Expect(err).NotTo(HaveOccurred())
		Expect(checksumBefore).NotTo(BeEmpty(), "apikey-checksum annotation not set on the serving Deployment")
		podsBefore, _ := utils.Kubectl("get", "pods", "-l", "decisionmodel.io/name="+dm,
			"-n", authNS, "-o", "jsonpath={.items[*].metadata.name}")

		By("rotating the key in place (same Secret, new value — no spec change)")
		applyYAML(fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: %s
  namespace: %s
  labels:
    decisionmodel.io/api-key: "true"
stringData:
  token: %s
`, secretName, authNS, newKey))

		By("the serving Deployment's apikey-checksum changes and the Pods roll " +
			"(RWO replicas:1 => Recreate, documented short outage)")
		// Pickup is on the ~60s regate reconcile; allow generous margin for the Recreate +
		// cold model reload on a CI runner. The old Pod name must be gone (rolled).
		Eventually(func(g Gomega) {
			checksumAfter, _ := utils.KubectlJSONPath(authNS, "deploy", dep,
				"{.metadata.annotations.decisionmodel\\.io/apikey-checksum}")
			g.Expect(checksumAfter).NotTo(Equal(checksumBefore),
				"apikey-checksum should change after the Secret value rotates")
			podsNow, _ := utils.Kubectl("get", "pods", "-l", "decisionmodel.io/name="+dm,
				"-n", authNS, "-o", "jsonpath={.items[*].metadata.name}")
			for _, old := range strings.Fields(podsBefore) {
				g.Expect(podsNow).NotTo(ContainSubstring(old),
					"old Pod %s should be replaced after rotation", old)
			}
		}, 5*time.Minute, 5*time.Second).Should(Succeed())

		By("the DM returns to Ready with the new key")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(authNS, "decisionmodel", dm, "{.status.phase}")
		}, 8*time.Minute, 5*time.Second).Should(Equal("Ready"),
			"DM did not return to Ready after key rotation")

		By("the new key is live (200), the old key is rejected (401)")
		stop := make(chan struct{})
		local := portForward(dm, authNS, 11435, stop)
		defer close(stop)
		body, _ := json.Marshal(map[string]any{
			"model": testModel, "keep_alive": -1,
			"state": evalStates[0], "questions": deptQuestion(),
		})
		Eventually(func(g Gomega) {
			code, err := utils.Run(exec.Command("curl", "-sS", "-o", "/dev/null", "-w", "%{http_code}",
				"--max-time", "180", "-X", "POST", fmt.Sprintf("http://%s/v1/systemone", local),
				"-H", "Content-Type: application/json",
				"-H", "Authorization: Bearer "+newKey, "--data-binary", string(body)))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(code).To(Equal("200"), "new key should be accepted after the roll")
		}, 3*time.Minute, 5*time.Second).Should(Succeed())

		code, err := utils.Run(exec.Command("curl", "-sS", "-o", "/dev/null", "-w", "%{http_code}",
			"--max-time", "180", "-X", "POST", fmt.Sprintf("http://%s/v1/systemone", local),
			"-H", "Content-Type: application/json",
			"-H", "Authorization: Bearer "+apiKeyValue, "--data-binary", string(body)))
		Expect(err).NotTo(HaveOccurred())
		Expect(code).To(Equal("401"), "the old key must be rejected after rotation")

		// NOTE: the "replicas: 2 on a shareable store never has zero ready endpoints" case
		// (RollingUpdate, no outage) needs an RWX StorageClass. The e2e kind cluster has only
		// the default RWO class, where the operator uses Recreate for replicas:1 — the short
		// outage asserted above is the documented RWO behaviour. The RWX no-zero-endpoints path
		// is left to a cluster that provides RWX (noted in the report).
	})

	It("rejects an unlabelled Secret: Ready=False reason SecretNotAllowed, no workloads", func() {
		By("creating an unlabelled Secret and a DM referencing it")
		applyYAML(fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata: {name: %s, namespace: %s}
stringData: {token: nope}
`, badSecretName, authNS))
		applyYAML(fmt.Sprintf(`apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata: {name: auth-bad, namespace: %s}
spec:
  engine: ollaya
  model: %s
  device: %s
  replicas: 1
  resources: {requests: {cpu: 250m, memory: 1Gi}, limits: {memory: 4Gi}}
  auth:
    apiKeySecretRef: {name: %s, key: token}
`, authNS, testModel, testDevice, badSecretName))

		By("Ready=False with reason SecretNotAllowed")
		Eventually(func(g Gomega) {
			status, _ := utils.KubectlJSONPath(authNS, "decisionmodel", "auth-bad",
				"{.status.conditions[?(@.type=='Ready')].status}")
			reason, _ := utils.KubectlJSONPath(authNS, "decisionmodel", "auth-bad",
				"{.status.conditions[?(@.type=='Ready')].reason}")
			g.Expect(status).To(Equal("False"))
			g.Expect(reason).To(Equal("SecretNotAllowed"))
		}, 2*time.Minute, 5*time.Second).Should(Succeed())

		By("no serving Deployment for the rejected DM")
		out, _ := utils.Kubectl("get", "deploy", "-l", "decisionmodel.io/name=auth-bad",
			"-n", authNS, "-o", "name")
		Expect(out).To(BeEmpty(), "expected no workloads for a rejected Secret")
	})
})

var _ = Describe("Eval-gated rollout", Ordered, func() {
	const dm = evalDM
	// verified maps each state to the label laya:en actually returns (learned live).
	var verified []string

	BeforeAll(func() {
		if testInstall == "helm" {
			Skip("helm job runs lifecycle specs only")
		}
		installAndDeploy()
		_, _ = utils.Kubectl("create", "ns", evalNS)

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
`, dm, evalNS, testModel, testDevice))
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(evalNS, "decisionmodel", dm, "{.status.phase}")
		}, 8*time.Minute, 5*time.Second).Should(Equal("Ready"))

		By("the stable revision owns a per-revision store PVC and no legacy shared store exists")
		// Each revision owns <dm>-store-<rev>; the legacy shared <dm>-store must not
		// be created for a freshly-provisioned DecisionModel.
		stableHash, err := utils.KubectlJSONPath(evalNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stableHash).NotTo(BeEmpty(), "status.stableRevision.hash not set")
		Expect(pvcExists(fmt.Sprintf("%s-store-%s", dm, stableHash))).To(BeTrue(),
			"per-revision store PVC %s-store-%s should exist", dm, stableHash)
		Expect(pvcExists(dm+"-store")).To(BeFalse(),
			"legacy shared store PVC %s-store must not exist for a new DecisionModel", dm)
		Expect(storePVCRevisions()).To(ConsistOf(stableHash),
			"exactly one store PVC (the stable revision's) should exist before any rollout")

		stop := make(chan struct{})
		local := portForward(dm, evalNS, 11435, stop)
		defer close(stop)
		verified = make([]string, len(evalStates))
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
			verified[i] = resp.Answers["q"].Choice
			_, _ = fmt.Fprintf(GinkgoWriter, "case %d -> %q\n", i, verified[i])
		}
	})

	AfterAll(func() {
		_, _ = utils.Kubectl("delete", "ns", evalNS, "--ignore-not-found")
		undeploy()
	})

	AfterEach(func() { dumpDiag(evalNS, dm) })

	It("kubectl get dm shows Active/Candidate/Accuracy/Phase/Reason columns at Ready", func() {
		// The printer columns are the primary at-a-glance UX. At steady state the
		// stable model is Active, there is no Candidate, and the Phase/Reason are Ready.
		// Free assertion: it reads the stable DM the BeforeAll already brought up.
		cols, err := dmColumns(evalNS, dm)
		Expect(err).NotTo(HaveOccurred())
		_, _ = fmt.Fprintf(GinkgoWriter, "columns at Ready: %+v\n", cols)
		Expect(cols["ACTIVE"]).To(Equal(testModel), "ACTIVE column should be the stable model")
		Expect(cols["PHASE"]).To(Equal("Ready"))
		Expect(cols["REASON"]).To(Equal("Ready"))
		Expect(cols["CANDIDATE"]).To(Or(Equal("<none>"), BeEmpty()),
			"CANDIDATE should be empty at steady state (got %q)", cols["CANDIDATE"])
	})

	It("promotes when the candidate meets minAccuracy (Evaluating -> Ready)", func() {
		By("recording the stable revision and its store PVC before the rollout")
		stableBefore, err := utils.KubectlJSONPath(evalNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stableBefore).NotTo(BeEmpty())

		By("creating a golden dataset the model gets right (verified labels)")
		applyConfigMap(evalNS, "golden-pass", "cases.jsonl", datasetJSONL(verified))

		By("disabling the post-promotion stabilization window for this GC assertion")
		// Default stabilization is 5m: after a promotion the previous revision's
		// Deployment and store PVC are intentionally kept for the window (instant
		// rollback). This spec asserts the demoted store PVC is collected shortly after
		// promotion, so set stabilization to "0s" (collected after the 30s endpoint grace,
		// the pre-stabilization behaviour). stabilization is not a revision-hash input, so
		// this patch does not itself create a revision.
		_, err = utils.Kubectl("patch", "decisionmodel", dm, "-n", evalNS, "--type=merge",
			"-p", `{"spec":{"rollout":{"stabilization":"0s"}}}`)
		Expect(err).NotTo(HaveOccurred())

		By("attaching evaluation and forcing a new revision (cpu bump)")
		patchEvalAndBump(dm, evalNS, "golden-pass", "0.8", "300m")

		By("while the candidate runs, both the stable and candidate store PVCs exist")
		// The candidate owns a second per-revision store PVC (footprint ~2x the model
		// during a blue-green rollout). Catch the two-PVC window before promotion.
		var candHash string
		Eventually(func(g Gomega) {
			h, _ := utils.KubectlJSONPath(evalNS, "decisionmodel", dm, "{.status.candidateRevision.hash}")
			g.Expect(h).NotTo(BeEmpty(), "candidateRevision.hash not set yet")
			candHash = h
			g.Expect(candHash).NotTo(Equal(stableBefore), "candidate must be a new revision")
			g.Expect(storePVCRevisions()).To(ConsistOf(stableBefore, candHash),
				"both stable and candidate store PVCs should coexist during the rollout")
		}, 5*time.Minute, 5*time.Second).Should(Succeed())

		By("phase passes through Evaluating and reaches Ready")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(evalNS, "decisionmodel", dm, "{.status.phase}")
		}, 12*time.Minute, 5*time.Second).Should(Equal("Ready"),
			"candidate should promote after passing eval")

		acc, err := utils.KubectlJSONPath(evalNS, "decisionmodel", dm, "{.status.evaluation.accuracy}")
		Expect(err).NotTo(HaveOccurred())
		Expect(acc).NotTo(BeEmpty(), "status.evaluation.accuracy should be recorded")
		_, _ = fmt.Fprintf(GinkgoWriter, "recorded accuracy=%s\n", acc)

		By("after promotion + the stabilization window, only the new revision's store PVC remains")
		// With stabilization disabled (set to "0" above), the demoted revision's store PVC
		// is garbage-collected once the endpoint grace (promoteGrace = 30s) elapses and the
		// stable path reconciles. (With the default 5m stabilization window it would be kept
		// for instant rollback — covered by the post-promotion rollback spec.) Poll for the
		// end state rather than sleeping the grace.
		Eventually(func(g Gomega) {
			stableNow, _ := utils.KubectlJSONPath(evalNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
			g.Expect(stableNow).To(Equal(candHash), "the candidate should now be the stable revision")
			g.Expect(storePVCRevisions()).To(ConsistOf(candHash),
				"only the promoted revision's store PVC should remain after the grace window")
			g.Expect(pvcExists(fmt.Sprintf("%s-store-%s", dm, stableBefore))).To(BeFalse(),
				"the demoted revision's store PVC %s-store-%s should be GC'd", dm, stableBefore)
		}, 3*time.Minute, 5*time.Second).Should(Succeed())
	})

	It("holds a candidate in Evaluating when its dataset ConfigMap is missing, then resumes on create", func() {
		// Reuses the stable DM this container already runs, so the per-PR cost is one
		// candidate model load (no extra stable). A candidate whose datasetRef names a
		// ConfigMap that does not exist yet must HOLD in Evaluating (reason
		// DatasetNotFound) rather than roll back, and must resume + promote once the
		// ConfigMap is created — no spec change, no retry annotation.
		By("recording the stable revision and Service selector before the rollout")
		stableBefore, err := utils.KubectlJSONPath(evalNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stableBefore).NotTo(BeEmpty())
		Expect(serviceRevision(evalNS, dm)).To(Equal(stableBefore), "Service should route to the stable revision")

		By("rolling out an eval-gated candidate whose dataset ConfigMap does not exist yet")
		// A generous Evaluating timeout so the hold does not fail-close while this test
		// creates the ConfigMap; minAccuracy is low so the candidate passes once the
		// (correct) dataset exists.
		patchRollout(dm, evalNS, rolloutPatch{
			cpu:          "320m",
			datasetCM:    "late-dataset",
			minAccuracy:  "0.5",
			evaluatingTO: "20m",
		})

		By("the candidate holds in Evaluating with Evaluated reason DatasetNotFound")
		var candHash string
		Eventually(func(g Gomega) {
			h, _ := utils.KubectlJSONPath(evalNS, "decisionmodel", dm, "{.status.candidateRevision.hash}")
			g.Expect(h).NotTo(BeEmpty(), "candidateRevision.hash not set yet")
			candHash = h
			phase, _ := utils.KubectlJSONPath(evalNS, "decisionmodel", dm, "{.status.phase}")
			g.Expect(phase).To(Equal("Evaluating"), "candidate should hold in Evaluating")
			reason, _ := utils.KubectlJSONPath(evalNS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Evaluated')].reason}")
			g.Expect(reason).To(Equal("DatasetNotFound"),
				"Evaluated reason should be DatasetNotFound while the dataset is missing")
		}, 10*time.Minute, 10*time.Second).Should(Succeed())

		By("the hold is stable: no rollback, no failedRevision, Service still on stable")
		Consistently(func(g Gomega) {
			phase, _ := utils.KubectlJSONPath(evalNS, "decisionmodel", dm, "{.status.phase}")
			g.Expect(phase).To(Equal("Evaluating"), "hold should stay in Evaluating")
			fr, _ := utils.KubectlJSONPath(evalNS, "decisionmodel", dm, "{.status.failedRevision.hash}")
			g.Expect(fr).To(BeEmpty(), "a hold must not set failedRevision")
			g.Expect(serviceRevision(evalNS, dm)).To(Equal(stableBefore),
				"Service must stay on the stable revision during the hold")
		}, 30*time.Second, 10*time.Second).Should(Succeed())

		By("the Candidate column is populated while the candidate holds")
		cols, err := dmColumns(evalNS, dm)
		Expect(err).NotTo(HaveOccurred())
		Expect(cols["CANDIDATE"]).To(Equal(testModel),
			"CANDIDATE should show the candidate model during a rollout (got %q)", cols["CANDIDATE"])
		Expect(cols["PHASE"]).To(Equal("Evaluating"))

		By("creating the dataset ConfigMap -> the held candidate resumes and promotes (no spec change, no retry)")
		applyConfigMap(evalNS, "late-dataset", "cases.jsonl", datasetJSONL(verified))
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(evalNS, "decisionmodel", dm, "{.status.phase}")
		}, 12*time.Minute, 5*time.Second).Should(Equal("Ready"),
			"candidate should promote once the dataset exists")
		stableNow, err := utils.KubectlJSONPath(evalNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stableNow).To(Equal(candHash), "the held candidate should now be the stable revision")
		Expect(serviceRevision(evalNS, dm)).To(Equal(candHash), "Service should route to the promoted revision")
	})

	It("rolls back when the candidate misses minAccuracy (RolledBack, stable keeps serving)", func() {
		By("waiting until only the promoted revision's Pod remains (old pod GC'd, memory freed)")
		// Blue-green briefly runs two model Pods (~3 GiB each). On a 2-vCPU CI runner that
		// memory pressure can push the next cold load past the controller's 10m startTimeout.
		// Wait for the old revision's Pod to be reclaimed so the eval-fail candidate loads with
		// headroom.
		Eventually(func(g Gomega) {
			out, err := utils.Kubectl("get", "pods", "-l", "decisionmodel.io/name="+dm,
				"-n", evalNS, "--field-selector=status.phase=Running",
				"-o", "jsonpath={.items[*].metadata.name}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.Fields(out)).To(HaveLen(1), "expected exactly one running serving Pod, got %q", out)
		}, 5*time.Minute, 10*time.Second).Should(Succeed())

		By("creating a golden dataset with deliberately wrong expected labels")
		applyConfigMap(evalNS, "golden-fail", "cases.jsonl", datasetJSONL(wrongLabels(verified)))

		By("recording the stable revision's identity before the failing rollout")
		// A failed candidate must not re-render the stable Deployment from
		// the candidate's (newer) spec. Capture the stable revision hash, its Deployment
		// spec-hash annotation and its serving container resources to compare after the
		// rollback.
		stableHashBefore, err := utils.KubectlJSONPath(evalNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stableHashBefore).NotTo(BeEmpty())
		specHashBefore, err := stableSpecHash(evalNS, dm, stableHashBefore)
		Expect(err).NotTo(HaveOccurred())
		Expect(specHashBefore).NotTo(BeEmpty(), "stable Deployment spec-hash annotation not set")
		resourcesBefore, err := stableContainerResources(evalNS, dm, stableHashBefore)
		Expect(err).NotTo(HaveOccurred())
		Expect(resourcesBefore).NotTo(BeEmpty())

		By("switching evaluation to the failing dataset and forcing a new revision")
		patchEvalAndBump(dm, evalNS, "golden-fail", "0.8", "350m")

		// Capture the new candidate revision hash so we can assert it is never promoted.
		var failRev string
		Eventually(func(g Gomega) {
			h, _ := utils.KubectlJSONPath(evalNS, "decisionmodel", dm, "{.status.candidateRevision.hash}")
			g.Expect(h).NotTo(BeEmpty())
			failRev = h
		}, 2*time.Minute, 5*time.Second).Should(Succeed())

		By("phase reaches RolledBack (contract)")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(evalNS, "decisionmodel", dm, "{.status.phase}")
		}, 12*time.Minute, 5*time.Second).Should(Equal("RolledBack"))

		By("eventually rolls back specifically for EvaluationFailed")
		// With the GC wait above the candidate warms within startTimeout, runs eval, scores
		// below minAccuracy and rolls back with reason EvaluationFailed. failedRevision is set.
		Eventually(func(g Gomega) {
			reason, _ := utils.KubectlJSONPath(evalNS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Degraded')].reason}")
			fr, _ := utils.KubectlJSONPath(evalNS, "decisionmodel", dm,
				"{.status.failedRevision.hash}")
			g.Expect(fr).NotTo(BeEmpty(), "failedRevision should be set")
			g.Expect(reason).To(Equal("EvaluationFailed"),
				"expected an EvaluationFailed rollback (got Degraded reason %q)", reason)
		}, 12*time.Minute, 10*time.Second).Should(Succeed())

		By("phase stays RolledBack (no stale-write regression / retry re-entry)")
		// Guards optimistic lock + guard-path phase: the failed revision is
		// terminal, phase must not regress to Caching/Evaluating.
		Consistently(func() (string, error) {
			return utils.KubectlJSONPath(evalNS, "decisionmodel", dm, "{.status.phase}")
		}, 30*time.Second, 5*time.Second).Should(Equal("RolledBack"))

		By("the failing candidate was not promoted (stable stays the passing revision)")
		stableHash, err := utils.KubectlJSONPath(evalNS, "decisionmodel", dm,
			"{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stableHash).NotTo(BeEmpty(), "a stable revision should remain")
		Expect(stableHash).NotTo(Equal(failRev), "failing candidate must not be promoted")

		By("the stable revision is unchanged (same hash, Deployment spec-hash and resources)")
		// The stable is rendered from status.stableRevision, never from the candidate's
		// spec, so the failed candidate (bumped to 350m) must not have rewritten the
		// stable Deployment's pod template.
		Expect(stableHash).To(Equal(stableHashBefore), "stable revision hash changed across a failed rollout")
		specHashAfter, err := stableSpecHash(evalNS, dm, stableHashBefore)
		Expect(err).NotTo(HaveOccurred())
		Expect(specHashAfter).To(Equal(specHashBefore),
			"stable Deployment spec-hash annotation changed after a failed candidate")
		resourcesAfter, err := stableContainerResources(evalNS, dm, stableHashBefore)
		Expect(err).NotTo(HaveOccurred())
		Expect(resourcesAfter).To(Equal(resourcesBefore),
			"stable Deployment serving-container resources changed after a failed candidate")

		By("the failed candidate's store PVC was garbage-collected")
		Eventually(func(g Gomega) {
			g.Expect(pvcExists(fmt.Sprintf("%s-store-%s", dm, failRev))).To(BeFalse(),
				"failed revision's store PVC %s-store-%s should be deleted", dm, failRev)
			g.Expect(storePVCRevisions()).To(ConsistOf(stableHashBefore),
				"only the surviving stable revision's store PVC should remain after rollback")
		}, 3*time.Minute, 5*time.Second).Should(Succeed())

		By("bounded EvaluationStarted events (no retry loop)")
		count, err := utils.Kubectl("get", "events", "-n", evalNS,
			"--field-selector", "reason=EvaluationStarted",
			"-o", "jsonpath={.items[*].reason}")
		Expect(err).NotTo(HaveOccurred())
		// One EvaluationStarted for the passing revision, one for the failing revision; a
		// retry loop would produce many more.
		Expect(strings.Count(count, "EvaluationStarted")).To(BeNumerically("<=", 2),
			"more than 2 EvaluationStarted events suggests a retry loop (got %q)", count)

		By("stable Service still answers after the failed rollout")
		readyReason, _ := utils.KubectlJSONPath(evalNS, "decisionmodel", dm,
			"{.status.conditions[?(@.type=='Ready')].reason}")
		_, _ = fmt.Fprintf(GinkgoWriter, "Ready reason after rollback=%s\n", readyReason)

		stop := make(chan struct{})
		local := portForward(dm, evalNS, 11435, stop)
		defer close(stop)
		Eventually(func(g Gomega) {
			code, err := utils.Run(exec.Command("curl", "-sS", "-o", "/dev/null", "-w", "%{http_code}",
				fmt.Sprintf("http://%s/", local)))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(code).To(Equal("200"), "stable Service stopped answering after rollback")
		}, 1*time.Minute, 5*time.Second).Should(Succeed())

		By("the EvaluationFailed Event names the model as model@sha256:<short> and says the stable keeps serving")
		// The operator-facing UX: a rejected candidate's Event identifies both revisions
		// by model@sha256:<short digest> and states that the stable revision keeps serving,
		// so an operator reading `kubectl get events` sees exactly what was rejected and
		// that traffic was never moved. Reuse the Event already produced by this rollback;
		// no extra model load.
		msg, err := utils.Kubectl("get", "events", "-n", evalNS,
			"--field-selector", "reason=EvaluationFailed",
			"-o", "jsonpath={.items[-1:].message}")
		Expect(err).NotTo(HaveOccurred())
		Expect(msg).To(ContainSubstring("@sha256:"),
			"EvaluationFailed Event should name the model as model@sha256:<short> (got %q)", msg)
		Expect(msg).To(ContainSubstring("keeps serving"),
			"EvaluationFailed Event should state the stable keeps serving (got %q)", msg)
	})
})

// ---- helpers ----

// installAndDeploy installs + deploys the operator via the E2E_INSTALL method and
// waits for it to be Available (shared with the lifecycle container).
func installAndDeploy() { installOperator() }

func undeploy() { uninstallOperator() }

// applyYAML pipes a manifest to `kubectl apply -f -`.
func applyYAML(manifest string) {
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = bytes.NewBufferString(manifest)
	out, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "kubectl apply: %s", out)
}

// applyConfigMap creates/updates a ConfigMap holding a single key.
//
//nolint:unparam // key kept explicit so call sites document the dataset key
func applyConfigMap(ns, name, key, value string) {
	cm := fmt.Sprintf(`apiVersion: v1
kind: ConfigMap
metadata: {name: %s, namespace: %s}
data:
  %s: |
`, name, ns, key)
	for _, line := range utils.SplitLines(value) {
		cm += "    " + line + "\n"
	}
	applyYAML(cm)
}

// patchEvalAndBump sets rollout.evaluation to the given dataset/minAccuracy and
// bumps cpu request to force a new revision (revision hash includes resources).
func patchEvalAndBump(name, ns, cmName, minAcc, cpu string) {
	patch := fmt.Sprintf(`{"spec":{"resources":{"requests":{"cpu":"%s","memory":"1Gi"},"limits":{"memory":"4Gi"}},`+
		`"rollout":{"evaluation":{"datasetRef":{"configMapRef":{"name":"%s","key":"cases.jsonl"}},"minAccuracy":"%s"}}}}`,
		cpu, cmName, minAcc)
	_, err := utils.Kubectl("patch", "decisionmodel", name, "-n", ns, "--type=merge", "-p", patch)
	Expect(err).NotTo(HaveOccurred())
}

// datasetJSONL builds a JSONL golden dataset: one choice question "q" per state,
// with the criteria used live and the given expected label per case.
func datasetJSONL(labels []string) string {
	var b bytes.Buffer
	for i, st := range evalStates {
		state, _ := json.Marshal(st)
		q, _ := json.Marshal(deptQuestion())
		exp, _ := json.Marshal(map[string]string{"q": labels[i]})
		fmt.Fprintf(&b, `{"state":%s,"questions":%s,"expected":%s}`+"\n", state, q, exp)
	}
	return b.String()
}

// wrongLabels returns a label per case guaranteed different from the verified one.
func wrongLabels(verified []string) []string {
	order := []string{"billing", "technical", "sales", "other"}
	out := make([]string, len(verified))
	for i, v := range verified {
		for _, cand := range order {
			if cand != v {
				out[i] = cand
				break
			}
		}
	}
	return out
}

// storePVCRevisions lists the revision hashes of the LIVE per-revision store PVCs
// of a DecisionModel.: each revision owns a PVC named <dm>-store-<rev>
// carrying labels decisionmodel.io/name=<dm> and decisionmodel.io/revision=<rev>.
// PVCs that are already being deleted (metadata.deletionTimestamp set) are
// excluded: once the operator has issued the delete, the PVC lingers in
// Terminating only until the last mounting Pod unmounts it (kubelet/CSI +
// kubernetes.io/pvc-protection), which is outside the operator's control and can
// take minutes on a loaded CI node. The returned slice holds the revision label
// of every store PVC of this DM that is not Terminating.
func storePVCRevisions() []string {
	// Emit "<revision> <deletionTimestamp>" per PVC and filter Terminating ones in
	// Go: kubectl jsonpath filters treat an absent field inconsistently, so we do
	// not rely on ?(@.metadata.deletionTimestamp=='').
	out, err := utils.Kubectl("get", "pvc",
		"-l", "decisionmodel.io/name="+evalDM, "-n", evalNS,
		"-o", "jsonpath={range .items[*]}{.metadata.labels.decisionmodel\\.io/revision}"+
			"{\" \"}{.metadata.deletionTimestamp}{\"\\n\"}{end}")
	Expect(err).NotTo(HaveOccurred())
	var revs []string
	for _, line := range utils.SplitLines(strings.TrimSpace(out)) {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue // blank line
		}
		if len(fields) >= 2 {
			continue // has a deletionTimestamp -> Terminating, treat as gone
		}
		revs = append(revs, fields[0])
	}
	return revs
}

// pvcExists reports whether a PVC with the given exact name exists and is LIVE
// (not being deleted). A missing PVC or one in Terminating (deletionTimestamp
// set) counts as not existing: once the operator issues the delete, the PVC may
// linger in Terminating until the last mounting Pod unmounts it (kubelet/CSI +
// kubernetes.io/pvc-protection), which is outside the operator's control.
func pvcExists(name string) bool {
	// --ignore-not-found makes a missing PVC return "" with no error. The output is
	// "<name>|" when live and "<name>|<timestamp>" when Terminating.
	out, err := utils.Kubectl("get", "pvc", name, "-n", evalNS, "--ignore-not-found",
		"-o", `jsonpath={.metadata.name}{"|"}{.metadata.deletionTimestamp}`)
	if err != nil {
		return false
	}
	out = strings.TrimSpace(out)
	if out == "" {
		return false // not found
	}
	// out is "<name>|" when live, "<name>|<timestamp>" when Terminating.
	parts := strings.SplitN(out, "|", 2)
	return len(parts) == 2 && parts[0] != "" && parts[1] == ""
}

// stableSpecHash returns the serving Deployment's decisionmodel.io/spec-hash
// annotation (set on the Deployment's metadata, not the pod template) for the
// given revision. A failed candidate must not change this for the stable revision.
func stableSpecHash(ns, dm, rev string) (string, error) {
	return utils.KubectlJSONPath(ns, "deploy", dm+"-"+rev,
		"{.metadata.annotations.decisionmodel\\.io/spec-hash}")
}

// stableContainerResources returns the serving container's resources block (as the
// raw jsonpath-rendered string) for the stable revision's Deployment.
func stableContainerResources(ns, dm, rev string) (string, error) {
	return utils.KubectlJSONPath(ns, "deploy", dm+"-"+rev,
		"{.spec.template.spec.containers[0].resources}")
}

func dumpDiag(ns, name string) {
	if !CurrentSpecReport().Failed() {
		return
	}
	res, _ := utils.Kubectl("get", "decisionmodel,deploy,pod,job,svc,pvc,cm,secret", "-n", ns, "-o", "wide")
	_, _ = fmt.Fprintf(GinkgoWriter, "resources in %s:\n%s\n", ns, res)
	desc, _ := utils.Kubectl("describe", "decisionmodel", name, "-n", ns)
	_, _ = fmt.Fprintf(GinkgoWriter, "DecisionModel describe:\n%s\n", desc)
	logs, _ := utils.Kubectl("logs", "-l", "control-plane=controller-manager", "-n", operatorNamespace, "--tail=120")
	_, _ = fmt.Fprintf(GinkgoWriter, "controller logs:\n%s\n", logs)
}
