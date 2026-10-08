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

// stabNS isolates the stabilization-window scenario.
const stabNS = "dmo-e2e-stabwin"

// This container proves the post-promotion stabilization window tolerates a
// legitimate in-place roll of the new stable — a replicas scale-up (whose new Pod
// cold-loads the model) and an API-key rotation — WITHOUT a false
// RolledBackAfterPromotion. While the stable Deployment is rolling, the operator
// classifies the quorum dip as StableRolling (via the Deployment's Progressing
// condition, reason != NewReplicaSetAvailable) rather than PostPromotionUnhealthy,
// so the rollback safety net does not fire; once the roll settles the window
// completes (Stabilized) and the DecisionModel stays Ready. Nightly (own model load).
var _ = Describe("Stabilization window: scale-up and key rotation", Label("nightly"), Ordered, func() {
	const dm = "stabwin-router"
	const secretName = "stabwin-key"
	const apiKey = "stabwin-key-v1"

	BeforeAll(func() {
		if testInstall == "helm" {
			Skip("helm job runs lifecycle specs only")
		}
		installAndDeploy()
		_, _ = utils.Kubectl("create", "ns", stabNS)

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
`, secretName, stabNS, apiKey))

		By("bringing up a stable revision with auth")
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
`, dm, stabNS, testModel, testDevice, secretName))
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(stabNS, "decisionmodel", dm, "{.status.phase}")
		}, 8*time.Minute, 5*time.Second).Should(Equal("Ready"))
	})

	AfterAll(func() {
		_, _ = utils.Kubectl("delete", "ns", stabNS, "--ignore-not-found")
		undeploy()
	})

	AfterEach(func() { dumpDiag(stabNS, dm) })

	It("tolerates a scale-up and a key rotation during the window without a false rollback", func() {
		stableBefore, err := utils.KubectlJSONPath(stabNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stableBefore).NotTo(BeEmpty())

		By("promoting a new revision with a 5m stabilization window (cpu bump)")
		_, err = utils.Kubectl("patch", "decisionmodel", dm, "-n", stabNS, "--type=merge",
			"-p", `{"spec":{"resources":{"requests":{"cpu":"300m","memory":"1Gi"},"limits":{"memory":"4Gi"}},`+
				`"rollout":{"stabilization":"5m"}}}`)
		Expect(err).NotTo(HaveOccurred())

		var candHash string
		By("waiting for the new revision to promote and the Stabilizing window to open")
		Eventually(func(g Gomega) {
			h, _ := utils.KubectlJSONPath(stabNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
			g.Expect(h).NotTo(Equal(stableBefore), "a new revision should be promoted")
			g.Expect(h).NotTo(BeEmpty())
			candHash = h
			stab, _ := utils.KubectlJSONPath(stabNS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Stabilizing')].status}")
			g.Expect(stab).To(Equal("True"), "the Stabilizing window should be open after promotion")
			prev, _ := utils.KubectlJSONPath(stabNS, "decisionmodel", dm, "{.status.previousRevision.hash}")
			g.Expect(prev).To(Equal(stableBefore), "the previous revision should be kept during the window")
		}, 12*time.Minute, 5*time.Second).Should(Succeed())
		dep := dm + "-" + candHash

		By("recording the serving Deployment and Stabilizing condition every 5s during the roll")
		// Start the sampler, then trigger both in-place changes: scale 1->2 (the new Pod
		// cold-loads the model for >30s) and rotate the API key (changes the apikey
		// checksum -> the Pods roll). Both make the Deployment Progressing with a reason
		// other than NewReplicaSetAvailable, which the operator must read as StableRolling.
		stop := make(chan struct{})
		samples := make(chan string, 256)
		go sampleDeployment(stabNS, dm, dep, samples, stop)

		By("scaling replicas 1->2 during the window")
		_, err = utils.Kubectl("patch", "decisionmodel", dm, "-n", stabNS, "--type=merge",
			"-p", `{"spec":{"replicas":2}}`)
		Expect(err).NotTo(HaveOccurred())

		By("rotating the API key in place during the window")
		applyYAML(fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: %s
  namespace: %s
  labels:
    decisionmodel.io/api-key: "true"
stringData:
  token: %s
`, secretName, stabNS, "stabwin-key-v2"))

		By("StableRolling must appear on the Stabilizing condition while the Deployment rolls")
		sawStableRolling := false
		Eventually(func(g Gomega) {
			reason, _ := utils.KubectlJSONPath(stabNS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Stabilizing')].reason}")
			if reason == "StableRolling" {
				sawStableRolling = true
			}
			// Never a false rollback while rolling.
			rb := eventReasonCountInNS(stabNS, dm, "RolledBackAfterPromotion")
			g.Expect(rb).To(Equal(0), "a legitimate in-place roll must not trigger a post-promotion rollback")
			g.Expect(sawStableRolling).To(BeTrue(),
				"the Stabilizing condition should report StableRolling while the stable Deployment rolls")
		}, 6*time.Minute, 3*time.Second).Should(Succeed())

		By("the window completes: Stabilized, Stabilizing clears, DM stays Ready, no rollback")
		Eventually(func(g Gomega) {
			stab, _ := utils.KubectlJSONPath(stabNS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Stabilizing')].status}")
			g.Expect(stab).NotTo(Equal("True"), "the Stabilizing condition should clear after the window")
			phase, _ := utils.KubectlJSONPath(stabNS, "decisionmodel", dm, "{.status.phase}")
			g.Expect(phase).To(Equal("Ready"))
		}, 8*time.Minute, 5*time.Second).Should(Succeed())
		close(stop)

		// Print the sampled Deployment timeline (generation / observedGeneration /
		// Progressing status+reason) so the sequence is quotable from the CI log.
		_, _ = fmt.Fprintf(GinkgoWriter, "=== serving Deployment %s timeline ===\n", dep)
		for {
			select {
			case line := <-samples:
				_, _ = fmt.Fprintln(GinkgoWriter, line)
			default:
				goto doneDraining
			}
		}
	doneDraining:
		Expect(eventReasonCountInNS(stabNS, dm, "RolledBackAfterPromotion")).To(Equal(0),
			"no post-promotion rollback should have fired across the scale-up + key rotation")
		Expect(eventReasonCountInNS(stabNS, dm, "Stabilized")).To(BeNumerically(">=", 1),
			"the window should have completed with a Stabilized Event")
		stableNow, _ := utils.KubectlJSONPath(stabNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(stableNow).To(Equal(candHash), "the promoted revision must remain the stable one")
		modelReady, _ := utils.KubectlJSONPath(stabNS, "decisionmodel", dm, "{.status.replicas.modelReady}")
		Expect(modelReady).To(Equal("2"), "both replicas should be model-ready after the scale-up")
	})
})

// ---- helpers ----

// sampleDeployment records the serving Deployment's generation, observedGeneration
// and Progressing condition (status + reason) every 5s until stop is closed, pushing
// one line per sample onto samples (non-blocking; drops if the buffer is full).
func sampleDeployment(ns, name, dep string, samples chan<- string, stop <-chan struct{}) {
	defer GinkgoRecover()
	for {
		select {
		case <-stop:
			return
		default:
		}
		gen, _ := utils.KubectlJSONPath(ns, "deploy", dep, "{.metadata.generation}")
		obs, _ := utils.KubectlJSONPath(ns, "deploy", dep, "{.status.observedGeneration}")
		pStatus, _ := utils.KubectlJSONPath(ns, "deploy", dep,
			"{.status.conditions[?(@.type=='Progressing')].status}")
		pReason, _ := utils.KubectlJSONPath(ns, "deploy", dep,
			"{.status.conditions[?(@.type=='Progressing')].reason}")
		stab, _ := utils.KubectlJSONPath(ns, "decisionmodel", name,
			"{.status.conditions[?(@.type=='Stabilizing')].reason}")
		line := fmt.Sprintf("%s  gen=%s observedGen=%s Progressing=%s/%s Stabilizing=%s",
			time.Now().UTC().Format(time.RFC3339), gen, obs, pStatus, pReason, stab)
		select {
		case samples <- line:
		default:
		}
		select {
		case <-stop:
			return
		case <-time.After(5 * time.Second):
		}
	}
}

// eventReasonCountInNS counts Events in a namespace on a DecisionModel by reason.
func eventReasonCountInNS(ns, name, reason string) int {
	out, _ := utils.Kubectl("get", "events", "-n", ns,
		"--field-selector", "involvedObject.name="+name+",reason="+reason,
		"-o", "jsonpath={.items[*].reason}")
	return len(strings.Fields(out))
}
