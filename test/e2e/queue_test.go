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
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/maks3201/decision-model-operator/test/utils"
)

// queueNS isolates the rollout-budget scenario.
const queueNS = "dmo-e2e-queue"

// queueDM is the DecisionModel whose stable must stay healthy while its candidate
// is parked behind the fleet rollout budget. queueHolder occupies the only slot.
const (
	queueDM     = "queue-router"
	queueHolder = "queue-holder"
)

// The rollout budget is tested with --max-concurrent-rollouts=1: at most one
// DecisionModel in the watched scope may have an in-flight candidate; others wait
// in phase Pending (Ready=False reason RolloutQueued). While queued, queueDM's
// serving stable must stay fully managed — its Deployment and Service are recreated
// if deleted and it keeps answering — and its candidate must NOT provision a store
// PVC or prefetch Job (it has not been admitted). Releasing the slot admits it.
var _ = Describe("Rollout budget: stable maintained while a candidate is queued",
	Label("queue"), Ordered, func() {

		// Approval token for the holder's parked candidate, learned in BeforeAll.
		var holderApproval string

		BeforeAll(func() {
			if testInstall == "helm" {
				Skip("helm job runs lifecycle specs only")
			}
			installAndDeploy()

			By("capping the fleet at one concurrent rollout")
			patchManagerArgs("--max-concurrent-rollouts=1")

			_, _ = utils.Kubectl("create", "ns", queueNS)

			By("bringing up queue-router to a serving stable FIRST (before the slot is taken)")
			// queue-router must own a serving stable before the holder occupies the
			// only slot, otherwise its very first revision would itself be queued.
			applyYAML(fmt.Sprintf(`apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata: {name: %s, namespace: %s}
spec:
  engine: ollaya
  model: %s
  device: %s
  replicas: 1
  resources: {requests: {cpu: 250m, memory: 1Gi}, limits: {memory: 4Gi}}
`, queueDM, queueNS, testModel, testDevice))
			Eventually(func() (string, error) {
				return utils.KubectlJSONPath(queueNS, "decisionmodel", queueDM, "{.status.phase}")
			}, 8*time.Minute, 5*time.Second).Should(Equal("Ready"))

			By("bringing up queue-holder and parking its candidate in AwaitingPromotion (holds the slot)")
			// Manual promotion + eval: the holder reaches a model-ready candidate that
			// waits for approval. A DM with a candidate set counts as an active rollout,
			// so it occupies the single slot until we approve it in the last step.
			applyYAML(fmt.Sprintf(`apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata: {name: %s, namespace: %s}
spec:
  engine: ollaya
  model: %s
  device: %s
  replicas: 1
  resources: {requests: {cpu: 250m, memory: 1Gi}, limits: {memory: 4Gi}}
`, queueHolder, queueNS, testModel, testDevice))
			Eventually(func() (string, error) {
				return utils.KubectlJSONPath(queueNS, "decisionmodel", queueHolder, "{.status.phase}")
			}, 8*time.Minute, 5*time.Second).Should(Equal("Ready"))
		})

		AfterAll(func() {
			_, _ = utils.Kubectl("delete", "ns", queueNS, "--ignore-not-found")
			undeploy()
		})

		AfterEach(func() { dumpDiag(queueNS, queueDM) })

		It("parks the holder, queues queue-router, keeps its stable healthy, admits it on release", func() {
			By("parking queue-holder in AwaitingPromotion to occupy the only rollout slot")
			// Eval-gated + manual promotion, cpu bump to force a candidate that passes
			// its (trivial) gate and then waits for approval, holding the slot.
			applyConfigMap(queueNS, "queue-golden", "cases.jsonl", holderDataset())
			patchRollout(queueHolder, queueNS, rolloutPatch{
				cpu:          "300m",
				datasetCM:    "queue-golden",
				minAccuracy:  "0.0",
				promotion:    "Manual",
				evaluatingTO: "30m",
			})
			Eventually(func() (string, error) {
				return utils.KubectlJSONPath(queueNS, "decisionmodel", queueHolder, "{.status.phase}")
			}, 10*time.Minute, 5*time.Second).Should(Equal("AwaitingPromotion"),
				"the holder must park in AwaitingPromotion to hold the slot")
			holderApproval, _ = utils.KubectlJSONPath(queueNS, "decisionmodel", queueHolder,
				"{.status.evaluation.approvalId}")
			Expect(holderApproval).NotTo(BeEmpty(), "holder approvalId not recorded")

			By("recording queue-router's stable revision and Service before it queues")
			stableRev, err := utils.KubectlJSONPath(queueNS, "decisionmodel", queueDM, "{.status.stableRevision.hash}")
			Expect(err).NotTo(HaveOccurred())
			Expect(stableRev).NotTo(BeEmpty())
			Expect(serviceRevision(queueNS, queueDM)).To(Equal(stableRev))
			stableDep := queueDM + "-" + stableRev

			By("changing queue-router's spec.model -> it must queue (Pending / RolloutQueued), slot is taken")
			_, err = utils.Kubectl("patch", "decisionmodel", queueDM, "-n", queueNS, "--type=merge",
				"-p", fmt.Sprintf(`{"spec":{"model":%q}}`, queuedCandidateModel()))
			Expect(err).NotTo(HaveOccurred())
			Eventually(func(g Gomega) {
				phase, _ := utils.KubectlJSONPath(queueNS, "decisionmodel", queueDM, "{.status.phase}")
				g.Expect(phase).To(Equal("Pending"), "queue-router should be Pending while the slot is taken")
				reason, _ := utils.KubectlJSONPath(queueNS, "decisionmodel", queueDM,
					"{.status.conditions[?(@.type=='Ready')].reason}")
				g.Expect(reason).To(Equal("RolloutQueued"), "Ready reason should be RolloutQueued")
			}, 5*time.Minute, 5*time.Second).Should(Succeed())

			By("while queued, the candidate has NO store PVC and NO prefetch Job")
			Consistently(func(g Gomega) {
				pvcs, _ := utils.Kubectl("get", "pvc", "-l", "decisionmodel.io/name="+queueDM,
					"-n", queueNS, "-o", "jsonpath={range .items[*]}{.metadata.labels.decisionmodel\\.io/revision}{\" \"}{end}")
				// Only the stable revision's store PVC may exist; no second (candidate) PVC.
				for _, rev := range strings.Fields(pvcs) {
					g.Expect(rev).To(Equal(stableRev), "a queued candidate must not own a store PVC (saw %q)", pvcs)
				}
				jobs, _ := utils.Kubectl("get", "job", "-l", "decisionmodel.io/name="+queueDM,
					"-n", queueNS, "-o", "jsonpath={range .items[*]}{.metadata.labels.decisionmodel\\.io/revision}{\" \"}{end}")
				for _, rev := range strings.Fields(jobs) {
					g.Expect(rev).To(Equal(stableRev), "a queued candidate must not own a prefetch Job (saw %q)", jobs)
				}
			}, 30*time.Second, 10*time.Second).Should(Succeed())

			By("deleting queue-router's stable Deployment while queued -> it is recreated and model-ready")
			_, err = utils.Kubectl("delete", "deploy", stableDep, "-n", queueNS, "--ignore-not-found")
			Expect(err).NotTo(HaveOccurred())
			Eventually(func(g Gomega) {
				_, derr := utils.KubectlJSONPath(queueNS, "deploy", stableDep, "{.metadata.name}")
				g.Expect(derr).NotTo(HaveOccurred(), "stable Deployment should be recreated while queued")
				ready, _ := utils.KubectlJSONPath(queueNS, "decisionmodel", queueDM, "{.status.replicas.modelReady}")
				g.Expect(ready).To(Equal("1"), "stable should be model-ready again after recreation")
				// Still queued, not flipped to Ready.
				phase, _ := utils.KubectlJSONPath(queueNS, "decisionmodel", queueDM, "{.status.phase}")
				g.Expect(phase).To(Equal("Pending"), "queue-router must stay queued while its stable is maintained")
			}, 8*time.Minute, 10*time.Second).Should(Succeed())

			By("the recreated stable answers /v1/systemone")
			// Re-establish the port-forward on each attempt: right after the stable Pod is
			// recreated the Service may briefly have no ready endpoint, which makes a held
			// port-forward fail without recovering. A fresh forward per try is robust.
			Eventually(func(g Gomega) {
				stop := make(chan struct{})
				local := portForward(queueDM, queueNS, 11435, stop)
				defer close(stop)
				code, cerr := utils.Run(exec.Command("curl", "-sS", "-o", "/dev/null", "-w", "%{http_code}",
					"--max-time", "20", fmt.Sprintf("http://%s/", local)))
				g.Expect(cerr).NotTo(HaveOccurred())
				g.Expect(code).To(Equal("200"), "recreated stable should answer")
			}, 5*time.Minute, 10*time.Second).Should(Succeed())

			By("deleting queue-router's Service while queued -> it is recreated selecting the stable revision")
			_, err = utils.Kubectl("delete", "svc", queueDM, "-n", queueNS, "--ignore-not-found")
			Expect(err).NotTo(HaveOccurred())
			Eventually(func(g Gomega) {
				sel := serviceRevision(queueNS, queueDM)
				g.Expect(sel).To(Equal(stableRev), "Service should be recreated selecting the stable revision")
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("releasing the slot: approve the holder -> queue-router is admitted and rolls out")
			_, err = utils.Kubectl("annotate", "decisionmodel", queueHolder, "-n", queueNS,
				"decisionmodel.io/promote="+holderApproval, "--overwrite")
			Expect(err).NotTo(HaveOccurred())
			Eventually(func() (string, error) {
				return utils.KubectlJSONPath(queueNS, "decisionmodel", queueHolder, "{.status.phase}")
			}, 8*time.Minute, 5*time.Second).Should(Equal("Ready"), "holder should promote on approval")

			By("queue-router leaves the queue and promotes its new model")
			Eventually(func() (string, error) {
				return utils.KubectlJSONPath(queueNS, "decisionmodel", queueDM, "{.status.phase}")
			}, 15*time.Minute, 10*time.Second).Should(Equal("Ready"),
				"queue-router should be admitted and reach Ready once the slot frees")
			newStable, err := utils.KubectlJSONPath(queueNS, "decisionmodel", queueDM, "{.status.stableRevision.hash}")
			Expect(err).NotTo(HaveOccurred())
			Expect(newStable).NotTo(Equal(stableRev), "queue-router should have rolled to a new revision after admission")
		})
	})

// ---- queue helpers ----

// queuedCandidateModel is the model queue-router is switched to in order to force a
// new (queued) candidate. A different valid tag changes the revision hash; while
// queued it is only resolved, never prefetched, so no extra weights are pulled until
// the DM is admitted. The per-PR suite uses laya:en; multilingual is the other small
// laya tag. For any other base model, multilingual still differs from the :en default.
func queuedCandidateModel() string {
	return "laya:multilingual"
}

// holderDataset returns a tiny golden set for the holder's trivial eval (minAccuracy
// 0.0 passes regardless), so it parks in AwaitingPromotion quickly.
func holderDataset() string {
	return `{"state":"my card was charged twice","questions":{"q":{"type":"choice",` +
		`"criteria":{"billing":"billing, refunds","other":"anything else"}}},"expected":{"q":"billing"}}` + "\n"
}
