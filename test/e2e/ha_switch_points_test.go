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

// swNS isolates these switch-point scenarios from the other HA container (which
// owns haNS) so the two can never share a namespace or a DecisionModel.
const swNS = "dmo-e2e-ha-switch"

// swDM is the DecisionModel (and Service) name used here.
const swDM = "sw-router"

// This container extends the HA coverage of "HA: leader-election failover"
// (ha_test.go) to the rollout points that container deliberately leaves out,
// because its phase comment notes they are either short-lived (Caching, Starting)
// or distinct lifecycle moments (right after promotion, during stable store
// recovery). It reuses that container's leader-election helpers
// (killLeaderAndAwaitTakeover, scaleManager, leaseHolder, learnLabels,
// patchRollout, serviceRevision, jobCountForRevision, deploymentCountForRevision,
// assertObservedGeneration, dumpManagerDiag) but runs in its own namespace with its
// own label so the two never scale the manager against each other.
//
// Each scenario kills the Lease-holding manager Pod at a specific point and asserts
// the surviving replica converges with no duplicated side effects:
//   - Caching / Starting — kill the leader the moment the candidate revision is
//     first recorded (the DM is resolving/caching/starting the candidate but has
//     not yet promoted). The rollout must still finish Ready with exactly one
//     prefetch Job and one serving Deployment for the candidate revision, i.e. the
//     failover did not restart the prefetch or create a duplicate candidate.
//   - right after promotion — kill the leader immediately after the candidate
//     becomes the stable revision (Service already switched). The promotion must not
//     be repeated and must not roll back: the stable stays the promoted revision and
//     the Promoted Event count gains exactly one for the whole rollout.
//   - during store recovery — delete the live stable store PVC and its Pod so the
//     operator must re-prefetch (store recovery), then kill the leader mid-recovery.
//     The new leader must finish recovery, keep the same stable revision, never
//     promote a new one, and the Service must always point at a revision with the
//     recovered store.
//
// Exactly-once is asserted on cluster STATE (one stableRevision pointing at the
// candidate, previousRevision == the prior stable, one Job/Deployment per revision)
// AND on the Promoted Event as a per-rollout DELTA (recorded just before the
// rollout, asserted +1 after), because the Promoted count is non-zero from the
// BeforeAll stable.
var _ = Describe("HA: leader-election failover at rollout switch points",
	Label("nightly", "ha-switch"), Ordered, func() {
		const dm = swDM

		BeforeAll(func() {
			if testInstall == "helm" {
				Skip("helm job runs lifecycle specs only")
			}
			installAndDeploy()

			By("running the controller-manager with two replicas (leader election already on)")
			scaleManager(2)
			Eventually(func(g Gomega) {
				g.Expect(managerReadyReplicas()).To(Equal(2), "both manager replicas should be Ready")
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("a leader holds the Lease")
			Eventually(func() string { return leaseHolder() }, 2*time.Minute, 5*time.Second).
				ShouldNot(BeEmpty(), "no leader acquired the Lease")

			_, _ = utils.Kubectl("create", "ns", swNS)

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
`, dm, swNS, testModel, testDevice))
			Eventually(func() (string, error) {
				return utils.KubectlJSONPath(swNS, "decisionmodel", dm, "{.status.phase}")
			}, 8*time.Minute, 5*time.Second).Should(Equal("Ready"))
		})

		AfterAll(func() {
			_, _ = utils.Kubectl("delete", "ns", swNS, "--ignore-not-found")
			// Restore a single manager replica so a reused cluster (local kind) is
			// left as the suite found it; the kustomize manifest default is 1.
			scaleManager(1)
			undeploy()
		})

		AfterEach(func() {
			dumpDiag(swNS, dm)
			dumpManagerDiag()
		})

		It("finishes a rollout after the leader is killed while the candidate is Caching/Starting", func() {
			stableBefore, err := utils.KubectlJSONPath(swNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
			Expect(err).NotTo(HaveOccurred())
			Expect(stableBefore).NotTo(BeEmpty())

			By("rolling out a new revision (no eval gate: promotion follows ModelReady)")
			promotedBefore := swEventCount("Promoted")
			swBumpCPU("350m")

			By("killing the Lease holder the moment the candidate revision is first recorded")
			// The candidate hash appears while the DM is Resolving/Caching/Starting and
			// has NOT yet promoted (Service still on the old stable). Killing here
			// exercises a failover across the short Caching/Starting window the primary
			// HA container cannot hit reliably.
			var candHash string
			Eventually(func(g Gomega) {
				h, _ := utils.KubectlJSONPath(swNS, "decisionmodel", dm, "{.status.candidateRevision.hash}")
				g.Expect(h).NotTo(BeEmpty(), "a candidate revision should be recorded")
				candHash = h
			}, 5*time.Minute, 2*time.Second).Should(Succeed())
			Expect(serviceRevision(swNS, dm)).To(Equal(stableBefore),
				"Service must still point at the old stable while the candidate is caching/starting")

			took := killLeaderAndAwaitTakeover()
			_, _ = fmt.Fprintf(GinkgoWriter, "leader failover (Caching/Starting) took %s\n", took)

			By("the rollout still finishes Ready on the new leader")
			Eventually(func() (string, error) {
				return utils.KubectlJSONPath(swNS, "decisionmodel", dm, "{.status.phase}")
			}, 10*time.Minute, 5*time.Second).Should(Equal("Ready"))
			stableNow, err := utils.KubectlJSONPath(swNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
			Expect(err).NotTo(HaveOccurred())
			Expect(stableNow).To(Equal(candHash), "the candidate started before the failover should become stable")
			Expect(serviceRevision(swNS, dm)).To(Equal(candHash), "Service should route to the promoted revision")

			By("no duplicate work: the failover did not restart the prefetch or fork the candidate")
			Expect(jobCountForRevision(swNS, dm, candHash)).To(Equal(1),
				"exactly one prefetch Job for the candidate revision across the leader change")
			Expect(deploymentCountForRevision(swNS, dm, candHash)).To(Equal(1),
				"exactly one serving Deployment should carry the candidate revision")
			Expect(len(deploymentRevisions(swNS, dm))).To(BeNumerically("<=", 2),
				"only the promoted and the kept-previous revision Deployments should exist")

			By("exactly one more Promoted Event than before this rollout")
			dumpTerminalEvents(swNS, dm, "Promoted")
			Expect(swEventCount("Promoted")).To(Equal(promotedBefore+1),
				"the rollout should add exactly one Promoted Event despite the failover")

			assertObservedGeneration(swNS, dm)
		})

		It("does not repeat or roll back a promotion after the leader is killed right after promotion", func() {
			stableBefore, err := utils.KubectlJSONPath(swNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
			Expect(err).NotTo(HaveOccurred())
			Expect(stableBefore).NotTo(BeEmpty())

			By("rolling out a new revision and waiting until it has just become the stable one")
			promotedBefore := swEventCount("Promoted")
			swBumpCPU("450m")

			// Wait until the candidate has been promoted: stableRevision advanced past
			// stableBefore and the Service already switched to it. This is the instant
			// right after promotion — the new stable is serving and the previous
			// revision's Deployment is still kept for the endpoint-gap grace.
			var candHash string
			Eventually(func(g Gomega) {
				h, _ := utils.KubectlJSONPath(swNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
				g.Expect(h).NotTo(BeEmpty())
				g.Expect(h).NotTo(Equal(stableBefore), "the new revision should have been promoted to stable")
				candHash = h
				g.Expect(serviceRevision(swNS, dm)).To(Equal(h), "Service should already route to the new stable")
			}, 12*time.Minute, 5*time.Second).Should(Succeed())

			By("killing the Lease holder right after promotion and asserting takeover")
			took := killLeaderAndAwaitTakeover()
			_, _ = fmt.Fprintf(GinkgoWriter, "leader failover (right after promotion) took %s\n", took)

			By("the new leader neither repeats the promotion nor rolls it back")
			// The promotion is durable in status.stableRevision; a new leader derives
			// state from the cluster and must not re-run it or revert to stableBefore.
			Consistently(func(g Gomega) {
				h, _ := utils.KubectlJSONPath(swNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
				g.Expect(h).To(Equal(candHash), "the stable revision must not change after the failover")
				g.Expect(serviceRevision(swNS, dm)).To(Equal(candHash), "Service must stay on the promoted revision")
			}, 30*time.Second, 5*time.Second).Should(Succeed())

			Eventually(func() (string, error) {
				return utils.KubectlJSONPath(swNS, "decisionmodel", dm, "{.status.phase}")
			}, 8*time.Minute, 5*time.Second).Should(Equal("Ready"))
			prev, err := utils.KubectlJSONPath(swNS, "decisionmodel", dm, "{.status.previousRevision.hash}")
			Expect(err).NotTo(HaveOccurred())
			Expect(prev).To(Equal(stableBefore),
				"previousRevision should be the stable that was serving before the candidate")

			By("exactly one more Promoted Event than before this rollout (not two)")
			dumpTerminalEvents(swNS, dm, "Promoted")
			Expect(swEventCount("Promoted")).To(Equal(promotedBefore+1),
				"a failover right after promotion must not emit a second Promoted Event")
			Expect(swEventCount("RolledBackAfterPromotion")).To(Equal(0),
				"a healthy promoted revision must not roll back because of a leader change")

			assertObservedGeneration(swNS, dm)
		})

		It("recovers the stable store and does not promote after the leader is killed mid store recovery", func() {
			stableBefore, err := utils.KubectlJSONPath(swNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
			Expect(err).NotTo(HaveOccurred())
			Expect(stableBefore).NotTo(BeEmpty())
			Expect(serviceRevision(swNS, dm)).To(Equal(stableBefore),
				"precondition: Service is on the current stable before recovery")

			By("deleting the live stable store PVC and its serving Pod to force store recovery")
			// Store recovery: with the store PVC gone the operator re-creates it and
			// re-runs the prefetch Job to rebuild the model store to the recorded
			// digest, keeping the SAME stable revision (no new revision, no promotion).
			pvc := dm + "-store-" + stableBefore
			_, _ = utils.Kubectl("delete", "pvc", pvc, "-n", swNS, "--wait=false")
			_, _ = utils.Kubectl("delete", "pods", "-l",
				"decisionmodel.io/name="+dm+",decisionmodel.io/revision="+stableBefore, "-n", swNS,
				"--grace-period=0", "--force", "--ignore-not-found")

			By("waiting until recovery is under way (a fresh prefetch Job or PVC for the stable revision)")
			Eventually(func(g Gomega) {
				jobs := jobCountForRevision(swNS, dm, stableBefore)
				live := swPVCExists(pvc)
				g.Expect(jobs > 0 || live).To(BeTrue(),
					"the operator should have begun recreating the store PVC / prefetch Job")
			}, 5*time.Minute, 3*time.Second).Should(Succeed())

			promotedBefore := swEventCount("Promoted")
			By("killing the Lease holder mid store recovery and asserting takeover")
			took := killLeaderAndAwaitTakeover()
			_, _ = fmt.Fprintf(GinkgoWriter, "leader failover (store recovery) took %s\n", took)

			By("the new leader finishes recovery back to Ready on the same stable revision")
			Eventually(func() (string, error) {
				return utils.KubectlJSONPath(swNS, "decisionmodel", dm, "{.status.phase}")
			}, 12*time.Minute, 5*time.Second).Should(Equal("Ready"),
				"store recovery must complete after the failover")
			stableNow, err := utils.KubectlJSONPath(swNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
			Expect(err).NotTo(HaveOccurred())
			Expect(stableNow).To(Equal(stableBefore),
				"store recovery rebuilds the store, it must not change the stable revision")
			Expect(serviceRevision(swNS, dm)).To(Equal(stableBefore),
				"Service must stay on the recovered stable revision throughout")

			By("recovery did not look like a rollout: no promotion during store recovery")
			Expect(swEventCount("Promoted")).To(Equal(promotedBefore),
				"a store recovery must not emit a Promoted Event (it is not a rollout)")
			cand, _ := utils.KubectlJSONPath(swNS, "decisionmodel", dm, "{.status.candidateRevision.hash}")
			Expect(strings.TrimSpace(cand)).To(BeEmpty(),
				"store recovery must not create a candidate revision")

			assertObservedGeneration(swNS, dm)
		})
	})

// swBumpCPU forces a new revision by bumping spec.resources.requests.cpu (the
// revision hash includes resources) WITHOUT setting spec.rollout.evaluation, so the
// candidate promotes on ModelReady alone (no eval gate). It is deliberately not
// patchRollout, which always writes a rollout.evaluation block.
func swBumpCPU(cpu string) {
	patch := fmt.Sprintf(
		`{"spec":{"resources":{"requests":{"cpu":%q,"memory":"1Gi"},"limits":{"memory":"4Gi"}}}}`, cpu)
	_, err := utils.Kubectl("patch", "decisionmodel", swDM, "-n", swNS, "--type=merge", "-p", patch)
	Expect(err).NotTo(HaveOccurred(), "failed to bump cpu to force a new revision")
}

// swEventCount counts distinct Events on the switch-point DecisionModel (swNS/swDM)
// with the given reason. It mirrors eventReasonCount (ha_test.go) but is scoped to
// this container's namespace and name, so the two HA containers never read each
// other's Events.
func swEventCount(reason string) int {
	out, _ := utils.Kubectl("get", "events", "-n", swNS,
		"--field-selector", "involvedObject.name="+swDM+",reason="+reason,
		"-o", "jsonpath={.items[*].reason}")
	return len(strings.Fields(out))
}

// swPVCExists reports whether a PVC exists and is not being deleted (a PVC in
// Terminating counts as gone, since its backing volume is on its way out).
func swPVCExists(name string) bool {
	ts, err := utils.KubectlJSONPath(swNS, "pvc", name, "{.metadata.deletionTimestamp}")
	if err != nil {
		return false // not found
	}
	return strings.TrimSpace(ts) == ""
}
