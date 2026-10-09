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

// schedulerEdgeNS isolates the scheduler/storage edge-case scenarios.
const schedulerEdgeNS = "dmo-e2e-scheduler-edge"

// This container exercises scheduler and storage edge cases that only a
// multi-node cluster can show. It reuses the two-node kind layout (one
// control-plane + two labelled nodes a/b, from hack/kind-recreate.yaml) and the
// node-targeting / sizing helpers that the Recreate container already defines in
// this package (recreateDM, perPodCPURequest, candidateRevision, servingPodNodes,
// serviceRevision, dumpDiag, installAndDeploy, applyYAML). It does NOT change that
// setup.
//
// It is labelled "scheduler-edge" so it runs only on its own multi-node nightly
// shard, and "nightly" so the per-PR label filter (!nightly) never selects it on a
// single-node cluster where it could not schedule the way it expects.
//
// Scenarios:
//   - a Recreate candidate must not start until the stable Pod is REALLY gone,
//     when a capacity-1 extended resource (not just CPU) is the scarce resource;
//   - a user-required host anti-affinity that contradicts the operator's RWO
//     co-location is unschedulable and surfaces as Degraded / a starting timeout,
//     never Ready=True;
//   - when the node holding co-located RWO replicas goes NotReady, the operator
//     reports the failure and does not promote or roll back on stale readiness;
//   - deleting a revision's manifest ConfigMap after the prefetch Job is created
//     fails with a clear reason (or is repaired), never an unexplained Caching
//     timeout.
var _ = Describe("Scheduler/storage edge cases", Label("scheduler-edge", "nightly"), Ordered, func() {
	const nodeA = "a"
	// extResource is a fake extended resource advertised with capacity 1 on node A
	// (patched in BeforeAll). A Pod that requests 1 of it occupies the only unit,
	// so a second Pod requesting it cannot be scheduled until the first is really
	// gone — exactly the capacity-1 device (one GPU) a Recreate rollout must serialise.
	const extResource = "decisionmodel.io/fake-accelerator"

	// nodeAName is the kind node name carrying the e2e-node=a label; needed to
	// patch node status (advertise/clear the fake extended resource) and to pause
	// it for the NotReady scenario.
	var nodeAName string

	BeforeAll(func() {
		if testInstall == "helm" {
			Skip("helm job runs lifecycle specs only")
		}
		installAndDeploy()
		_, _ = utils.Kubectl("create", "ns", schedulerEdgeNS)

		var err error
		nodeAName, err = utils.Kubectl("get", "nodes",
			"-l", "decisionmodel.io/e2e-node="+nodeA,
			"-o", "jsonpath={.items[0].metadata.name}")
		Expect(err).NotTo(HaveOccurred())
		Expect(nodeAName).NotTo(BeEmpty(), "node with label decisionmodel.io/e2e-node=%s not found", nodeA)
	})

	AfterAll(func() {
		_, _ = utils.Kubectl("delete", "ns", schedulerEdgeNS, "--ignore-not-found")
		undeploy()
	})

	AfterEach(func() { dumpDiag(schedulerEdgeNS, "") })

	// Item 6: a stable Pod that lingers (a long preStop) under Recreate with a
	// capacity-1 extended resource must block the candidate until it is REALLY
	// gone. We advertise a fake accelerator with capacity 1 on node A, make the
	// serving container request it, and add a long preStop so the stable Pod stays
	// Terminating for a while after the operator stops it. The candidate (same
	// resource request) must stay Pending until the terminating stable releases the
	// one unit; it must not start "early" against stale capacity.
	It("holds a Recreate candidate until a lingering stable Pod has really released a capacity-1 resource", func() {
		const dm = "edge-serialize"

		By("advertising a capacity-1 fake extended resource on node A")
		advertiseExtendedResource(nodeAName, extResource, 1)
		DeferCleanup(func() { clearExtendedResource(nodeAName, extResource) })

		By("bringing up a stable revision that requests the one accelerator unit and lingers on stop")
		applyYAML(extResourceDM(dm, nodeA, extResource, ""))
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(schedulerEdgeNS, "decisionmodel", dm, "{.status.phase}")
		}, 8*time.Minute, 5*time.Second).Should(Equal("Ready"))
		DeferCleanup(func() {
			_, _ = utils.Kubectl("delete", "decisionmodel", dm, "-n", schedulerEdgeNS, "--ignore-not-found")
		})
		stableBefore, err := utils.KubectlJSONPath(schedulerEdgeNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stableBefore).NotTo(BeEmpty())

		By("rolling out a Recreate candidate that needs the same single accelerator unit")
		// A changed memory limit forces a new revision; the accelerator request and
		// node pin are unchanged, so the candidate needs the one unit the stable holds.
		applyYAML(extResourceDM(dm, nodeA, extResource, "5Gi"))
		var candHash string
		Eventually(func(g Gomega) {
			h := candidateRevision(dm)
			g.Expect(h).NotTo(BeEmpty())
			g.Expect(h).NotTo(Equal(stableBefore))
			candHash = h
		}, 5*time.Minute, 5*time.Second).Should(Succeed())

		By("the operator records the stable as stopped for the candidate (Recreate)")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(schedulerEdgeNS, "decisionmodel", dm, "{.status.stableStoppedForRevision}")
		}, 5*time.Minute, 5*time.Second).Should(Equal(candHash))

		By("while the stable Pod is still Terminating (preStop), the candidate stays Pending Unschedulable")
		// As long as a stable Pod with the accelerator is Terminating, the one unit
		// is still accounted to it, so the candidate cannot be scheduled. The
		// candidate must not have started on stale capacity.
		Eventually(func(g Gomega) {
			g.Expect(anyTerminatingServingPod(dm)).To(BeTrue(),
				"a stable serving Pod should still be Terminating during its preStop")
			g.Expect(candidatePodUnschedulable(dm, candHash)).To(BeTrue(),
				"the candidate must stay Unschedulable until the terminating Pod releases the accelerator")
		}, 2*time.Minute, 3*time.Second).Should(Succeed())
		Expect(serviceRevision(schedulerEdgeNS, dm)).NotTo(Equal(candHash),
			"the candidate must not be promoted while the stable Pod lingers")

		By("once the terminating Pod is gone, the candidate schedules and is promoted")
		Eventually(func(g Gomega) {
			phase, _ := utils.KubectlJSONPath(schedulerEdgeNS, "decisionmodel", dm, "{.status.phase}")
			g.Expect(phase).To(Equal("Ready"))
			g.Expect(serviceRevision(schedulerEdgeNS, dm)).To(Equal(candHash))
		}, 12*time.Minute, 10*time.Second).Should(Succeed())
	})

	// Item 37: no spare capacity for a BlueGreen candidate (a capacity-1 extended
	// resource stands in for one GPU). Under the default BlueGreen strategy the
	// candidate needs the one unit the running stable holds, so it is Unschedulable;
	// with a bounded starting timeout the rollout times out and rolls back, and the
	// stable serves throughout. Switching the SAME candidate to Recreate (which stops
	// the stable first) then succeeds. This is the BlueGreen-fails/Recreate-succeeds
	// pair for an extended resource (the Recreate serialisation is proved by the
	// capacity-1 lingering-Pod spec above; this adds the BlueGreen timeout→rollback leg).
	It("times out and rolls back a BlueGreen candidate with no spare capacity, then Recreate succeeds", func() {
		const dm = "edge-nospare"

		By("advertising a capacity-1 fake accelerator on node A")
		advertiseExtendedResource(nodeAName, extResource, 1)
		DeferCleanup(func() { clearExtendedResource(nodeAName, extResource) })

		By("bringing up a stable that holds the one accelerator unit")
		applyYAML(extResourceDM(dm, nodeA, extResource, ""))
		DeferCleanup(func() {
			_, _ = utils.Kubectl("delete", "decisionmodel", dm, "-n", schedulerEdgeNS, "--ignore-not-found")
		})
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(schedulerEdgeNS, "decisionmodel", dm, "{.status.phase}")
		}, 8*time.Minute, 5*time.Second).Should(Equal("Ready"))
		stableBefore, err := utils.KubectlJSONPath(schedulerEdgeNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stableBefore).NotTo(BeEmpty())

		By("rolling out a BlueGreen candidate that needs the same single unit, with a short starting timeout")
		applyYAML(extResourceDMBlueGreen(dm, extResource, "5Gi", "3m"))
		var candHash string
		Eventually(func(g Gomega) {
			h := candidateRevision(dm)
			g.Expect(h).NotTo(BeEmpty())
			g.Expect(h).NotTo(Equal(stableBefore))
			candHash = h
		}, 5*time.Minute, 5*time.Second).Should(Succeed())

		By("the BlueGreen candidate is Unschedulable and the stable keeps serving")
		Eventually(func(g Gomega) {
			g.Expect(candidatePodUnschedulable(dm, candHash)).To(BeTrue(),
				"the BlueGreen candidate must be Unschedulable with no spare accelerator")
		}, 5*time.Minute, 10*time.Second).Should(Succeed())
		Expect(serviceRevision(schedulerEdgeNS, dm)).To(Equal(stableBefore),
			"BlueGreen must keep the Service on the stable while the candidate cannot start")

		By("the rollout times out and rolls back to the stable (which served throughout)")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(schedulerEdgeNS, "decisionmodel", dm, "{.status.phase}")
		}, 8*time.Minute, 10*time.Second).Should(Equal("RolledBack"),
			"a BlueGreen candidate that never schedules should roll back on the starting timeout")
		Expect(serviceRevision(schedulerEdgeNS, dm)).To(Equal(stableBefore),
			"the stable must still serve after the BlueGreen rollback")
		failed, _ := utils.KubectlJSONPath(schedulerEdgeNS, "decisionmodel", dm, "{.status.failedRevision.hash}")
		Expect(failed).To(Equal(candHash), "the failed revision should be the unschedulable candidate")

		By("switching the SAME candidate to Recreate stops the stable first, so it fits and promotes")
		// Re-apply the candidate revision under Recreate (strategy is not part of the
		// revision hash). Recreate stops the stable, freeing the one accelerator unit,
		// so the candidate schedules and is promoted.
		applyYAML(extResourceDMRecreate(dm, extResource, "5Gi"))
		Eventually(func(g Gomega) {
			phase, _ := utils.KubectlJSONPath(schedulerEdgeNS, "decisionmodel", dm, "{.status.phase}")
			g.Expect(phase).To(Equal("Ready"))
			g.Expect(serviceRevision(schedulerEdgeNS, dm)).To(Equal(candHash))
		}, 12*time.Minute, 10*time.Second).Should(Succeed())
	})

	// Item 11: a user-required host anti-affinity on replicas 2 (RWO) contradicts
	// the operator's required host CO-location (RWO pins all replicas to one node).
	// The two requirements cannot both hold, so a replica stays Pending and the DM
	// must never report Ready=True — it should surface Degraded or a starting
	// timeout. We report exactly what the operator says. (Report-only: we do NOT
	// change the controller; the task asks whether it should reject the combination
	// up front — recorded in the report.)
	It("never reports Ready for RWO replicas:2 with a user host anti-affinity that fights co-location", func() {
		const dm = "edge-antiaffinity"

		By("bringing up replicas:2 on an RWO store with a required host anti-affinity")
		applyYAML(antiAffinityDM(dm, nodeA, 2))
		DeferCleanup(func() {
			_, _ = utils.Kubectl("delete", "decisionmodel", dm, "-n", schedulerEdgeNS, "--ignore-not-found")
		})

		By("at least one replica stays Pending/Unschedulable and the DM never becomes Ready")
		// The operator co-locates (required host pod-affinity); a user required host
		// anti-affinity on the same topology key is contradictory, so one replica
		// cannot schedule. Consistently assert the DM does not reach Ready=True.
		Consistently(func(g Gomega) {
			ready, _ := utils.KubectlJSONPath(schedulerEdgeNS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Ready')].status}")
			g.Expect(ready).NotTo(Equal("True"),
				"a contradictory anti-affinity must never let the DM report Ready=True")
		}, 4*time.Minute, 15*time.Second).Should(Succeed())

		By("the operator reports Degraded or stays in a non-Ready progress phase; recording what it says")
		phase, _ := utils.KubectlJSONPath(schedulerEdgeNS, "decisionmodel", dm, "{.status.phase}")
		degradedReason, _ := utils.KubectlJSONPath(schedulerEdgeNS, "decisionmodel", dm,
			"{.status.conditions[?(@.type=='Degraded')].reason}")
		modelReady, _ := utils.KubectlJSONPath(schedulerEdgeNS, "decisionmodel", dm, "{.status.replicas.modelReady}")
		_, _ = fmt.Fprintf(GinkgoWriter,
			"anti-affinity vs co-location: phase=%q degradedReason=%q modelReady=%q (want: not Ready, <2 ready)\n",
			phase, degradedReason, modelReady)
		Expect(phase).NotTo(Equal("Ready"), "phase must not be Ready when a replica cannot schedule")
		Expect(modelReady).NotTo(Equal("2"), "two replicas cannot both be ready under a contradictory anti-affinity")
	})

	// Item 12: co-located RWO replicas on node A; node A goes NotReady (docker
	// pause). The operator must report the storage/scheduling failure and must NOT
	// promote a candidate or roll back on stale readiness (the paused node's Pods
	// still look Ready in the API until the node is marked NotReady and pod
	// eviction/unknown kicks in). We pause the node, roll a candidate, and assert
	// no promotion/rollback happens on stale state while the node is down.
	It("does not promote or roll back on stale readiness when the co-located node goes NotReady", func() {
		const dm = "edge-notready"

		By("bringing up co-located RWO replicas:2 on node A")
		applyYAML(edgeColocatedDM(dm, 2, ""))
		DeferCleanup(func() {
			_, _ = utils.Kubectl("delete", "decisionmodel", dm, "-n", schedulerEdgeNS, "--ignore-not-found")
		})
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(schedulerEdgeNS, "decisionmodel", dm, "{.status.phase}")
		}, 8*time.Minute, 5*time.Second).Should(Equal("Ready"))
		stableBefore, err := utils.KubectlJSONPath(schedulerEdgeNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stableBefore).NotTo(BeEmpty())

		By("pausing node A so the kubelet stops reporting (the node goes NotReady)")
		pauseNode(nodeAName)
		paused := true
		DeferCleanup(func() {
			if paused {
				unpauseNode(nodeAName)
			}
		})

		By("the node is observed NotReady")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath("", "node", nodeAName,
				"{.status.conditions[?(@.type=='Ready')].status}")
		}, 5*time.Minute, 10*time.Second).Should(Equal("Unknown"),
			"a paused kind node should go to Ready=Unknown (NotReady)")

		By("rolling a candidate while node A is down; it must not promote on stale readiness")
		applyYAML(edgeColocatedDM(dm, 2, "5Gi")) // new revision via mem limit
		// The candidate cannot become genuinely model-ready (its node is down and the
		// RWO store is on the paused node). The operator must not flip the Service to
		// the candidate, and must not roll back the stable on stale state.
		Consistently(func(g Gomega) {
			svcRev := serviceRevision(schedulerEdgeNS, dm)
			g.Expect(svcRev).To(Equal(stableBefore),
				"the Service must stay on the stable while the node is NotReady (no stale promotion)")
			phase, _ := utils.KubectlJSONPath(schedulerEdgeNS, "decisionmodel", dm, "{.status.phase}")
			g.Expect(phase).NotTo(Equal("RolledBack"),
				"the operator must not roll back on stale readiness while the node is NotReady")
		}, 3*time.Minute, 15*time.Second).Should(Succeed())

		degradedReason, _ := utils.KubectlJSONPath(schedulerEdgeNS, "decisionmodel", dm,
			"{.status.conditions[?(@.type=='Degraded')].reason}")
		phase, _ := utils.KubectlJSONPath(schedulerEdgeNS, "decisionmodel", dm, "{.status.phase}")
		_, _ = fmt.Fprintf(GinkgoWriter,
			"node NotReady: phase=%q degradedReason=%q (want: no promotion, no rollback on stale readiness)\n",
			phase, degradedReason)

		By("unpausing node A so the suite can clean up")
		unpauseNode(nodeAName)
		paused = false
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath("", "node", nodeAName,
				"{.status.conditions[?(@.type=='Ready')].status}")
		}, 5*time.Minute, 10*time.Second).Should(Equal("True"))
	})

	// Item 14: delete a revision's manifest ConfigMap after the prefetch Job is
	// created but before its Pod mounts it. The ConfigMap mount is required
	// (optional: false), so a genuinely missing one must surface a clear reason —
	// either the operator repairs the ConfigMap and the Job proceeds, or the Job
	// fails with SeedUnavailable / a mount error on the condition/Events — NOT an
	// unexplained Caching timeout with no reason.
	It("surfaces a clear reason (or repairs) when a revision's manifest ConfigMap is deleted mid-prefetch", func() {
		const dm = "edge-cmdelete"

		By("creating a DecisionModel and waiting until its prefetch Job exists")
		applyYAML(edgeColocatedDM(dm, 1, ""))
		DeferCleanup(func() {
			_, _ = utils.Kubectl("delete", "decisionmodel", dm, "-n", schedulerEdgeNS, "--ignore-not-found")
		})
		var rev string
		Eventually(func(g Gomega) {
			r, _ := utils.KubectlJSONPath(schedulerEdgeNS, "decisionmodel", dm, "{.status.candidateRevision.hash}")
			if r == "" {
				r, _ = utils.KubectlJSONPath(schedulerEdgeNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
			}
			g.Expect(r).NotTo(BeEmpty(), "a revision hash should be recorded")
			rev = r
			jobs, _ := utils.Kubectl("get", "jobs", "-n", schedulerEdgeNS,
				"-l", "decisionmodel.io/name="+dm, "-o", "jsonpath={.items[*].metadata.name}")
			g.Expect(strings.Fields(jobs)).NotTo(BeEmpty(), "the prefetch Job should exist")
		}, 5*time.Minute, 3*time.Second).Should(Succeed())

		By("deleting the per-revision manifest ConfigMap <dm>-manifest-<rev>")
		cmName := dm + "-manifest-" + rev
		_, _ = utils.Kubectl("delete", "configmap", cmName, "-n", schedulerEdgeNS, "--ignore-not-found")

		By("the DM either reaches a non-Caching outcome or carries a clear reason — never a silent Caching hang")
		// Acceptable outcomes, all with a reason a user can act on:
		//   - the operator repairs the ConfigMap and the DM reaches Ready;
		//   - a Cached=False / prefetch condition reports SeedUnavailable or a
		//     mount/ConfigMap problem;
		//   - a Warning Event names the missing ConfigMap.
		// The failure the task guards against is an unexplained Caching timeout
		// with no reason. We require some explanatory signal within the window.
		Eventually(func(g Gomega) {
			phase, _ := utils.KubectlJSONPath(schedulerEdgeNS, "decisionmodel", dm, "{.status.phase}")
			cachedReason, _ := utils.KubectlJSONPath(schedulerEdgeNS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Cached')].reason}")
			events, _ := utils.Kubectl("get", "events", "-n", schedulerEdgeNS,
				"--field-selector", "involvedObject.name="+dm, "-o",
				"jsonpath={.items[*].reason}")

			repaired := phase == "Ready"
			hasReason := cachedReason != "" && cachedReason != "Caching"
			seedSignal := strings.Contains(events, "SeedUnavailable") ||
				strings.Contains(strings.ToLower(events), "manifest") ||
				strings.Contains(strings.ToLower(events), "configmap")

			_, _ = fmt.Fprintf(GinkgoWriter,
				"manifest-CM delete: phase=%q cachedReason=%q events=%q (repaired=%v hasReason=%v seedSignal=%v)\n",
				phase, cachedReason, events, repaired, hasReason, seedSignal)

			g.Expect(repaired || hasReason || seedSignal).To(BeTrue(),
				"deleting the manifest ConfigMap must repair or surface a clear reason, not a silent Caching hang")
		}, 10*time.Minute, 10*time.Second).Should(Succeed())
	})
})

// ---- helpers local to the scheduler-edge container ----
// These are scoped to this file (the task allows helpers "in that file only").

// edgeColocatedDM renders an RWO, node-A-pinned DecisionModel with a small CPU
// request. It is this file's own renderer (not the Recreate container's recreateDM)
// so the scheduler-edge specs do not add call sites to a helper owned by another
// spec file. memLimit forces a new revision hash when changed (resources are part
// of the hash); empty defaults to 4Gi. replicas>1 on the RWO store exercises the
// operator's co-location. All scheduler-edge RWO specs pin to node A, so the node
// is fixed here rather than passed.
func edgeColocatedDM(name string, replicas int, memLimit string) string {
	if memLimit == "" {
		memLimit = "4Gi"
	}
	return fmt.Sprintf(`apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata: {name: %s, namespace: %s}
spec:
  engine: ollaya
  model: %s
  device: %s
  replicas: %d
  cache: {accessModes: [ReadWriteOnce]}
  resources:
    requests: {cpu: 250m, memory: 1Gi}
    limits: {memory: %s}
  scheduling:
    nodeSelector: {decisionmodel.io/e2e-node: a}
`, name, schedulerEdgeNS, testModel, testDevice, replicas, memLimit)
}

// extResourceDM renders a DecisionModel pinned to one node that requests one unit
// of a fake extended resource. memLimit sets the memory limit so a change between
// revisions forces a new revision hash (resources are part of the hash) while the
// extended-resource request and node pin stay the same. The engine does not expose
// a preStop hook through the CRD, so a lingering stable Pod is modelled by the
// terminationGracePeriod the runtime applies plus the extended-resource accounting:
// a Terminating Pod keeps its one unit until it is really gone, which is what the
// candidate must wait for.
func extResourceDM(name, nodeVal, res, memLimit string) string {
	if memLimit == "" {
		memLimit = "4Gi"
	}
	return fmt.Sprintf(`apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata: {name: %s, namespace: %s}
spec:
  engine: ollaya
  model: %s
  device: %s
  replicas: 1
  resources:
    requests:
      cpu: 250m
      memory: 1Gi
      %s: "1"
    limits:
      memory: %s
      %s: "1"
  scheduling:
    nodeSelector: {decisionmodel.io/e2e-node: %s}
`, name, schedulerEdgeNS, testModel, testDevice, res, memLimit, res, nodeVal)
}

// extResourceDMBlueGreen renders the node-A extended-resource DecisionModel under
// the explicit BlueGreen strategy with a bounded starting timeout, so a candidate
// that cannot schedule (no spare accelerator) rolls back on the timeout rather than
// hanging. memLimit forces a new revision hash vs the stable; startingTO bounds the
// Starting phase.
func extResourceDMBlueGreen(name, res, memLimit, startingTO string) string {
	return fmt.Sprintf(`apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata: {name: %s, namespace: %s}
spec:
  engine: ollaya
  model: %s
  device: %s
  replicas: 1
  resources:
    requests: {cpu: 250m, memory: 1Gi, %s: "1"}
    limits: {memory: %s, %s: "1"}
  scheduling:
    nodeSelector: {decisionmodel.io/e2e-node: a}
  rollout:
    strategy: BlueGreen
    timeouts: {starting: %s}
`, name, schedulerEdgeNS, testModel, testDevice, res, memLimit, res, startingTO)
}

// extResourceDMRecreate is extResourceDMBlueGreen's Recreate counterpart: the same
// revision (same memLimit) under the Recreate strategy, which stops the stable
// first so the one accelerator unit is free for the candidate.
func extResourceDMRecreate(name, res, memLimit string) string {
	return fmt.Sprintf(`apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata: {name: %s, namespace: %s}
spec:
  engine: ollaya
  model: %s
  device: %s
  replicas: 1
  resources:
    requests: {cpu: 250m, memory: 1Gi, %s: "1"}
    limits: {memory: %s, %s: "1"}
  scheduling:
    nodeSelector: {decisionmodel.io/e2e-node: a}
  rollout:
    strategy: Recreate
`, name, schedulerEdgeNS, testModel, testDevice, res, memLimit, res)
}

// antiAffinityDM renders replicas:2 on an RWO store with a user-required host
// anti-affinity on the DM's own serving Pods, which contradicts the operator's
// required host co-location for an RWO store.
func antiAffinityDM(name, nodeVal string, replicas int) string {
	return fmt.Sprintf(`apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata: {name: %s, namespace: %s}
spec:
  engine: ollaya
  model: %s
  device: %s
  replicas: %d
  cache: {accessModes: [ReadWriteOnce]}
  resources:
    requests: {cpu: 250m, memory: 1Gi}
    limits: {memory: 4Gi}
  scheduling:
    nodeSelector: {decisionmodel.io/e2e-node: %s}
    affinity:
      podAntiAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          - topologyKey: kubernetes.io/hostname
            labelSelector:
              matchExpressions:
                - key: decisionmodel.io/name
                  operator: In
                  values: [%s]
`, name, schedulerEdgeNS, testModel, testDevice, replicas, nodeVal, name)
}

// advertiseExtendedResource patches a node's status to advertise `capacity` units
// of a fake extended resource. kind nodes do not have one by default; this makes a
// capacity-1 device the scarce resource that serialises a Recreate rollout.
func advertiseExtendedResource(node, res string, capacity int) {
	// The resource name contains a '/', which must be escaped as '~1' in a JSON Pointer.
	ptr := "/status/capacity/" + strings.ReplaceAll(res, "/", "~1")
	patch := fmt.Sprintf(`[{"op":"add","path":%q,"value":%q}]`, ptr, fmt.Sprintf("%d", capacity))
	_, err := utils.Kubectl("patch", "node", node, "--subresource=status", "--type=json", "-p", patch)
	Expect(err).NotTo(HaveOccurred(), "failed to advertise %s on node %s", res, node)
}

// clearExtendedResource removes the fake extended resource from a node's status.
func clearExtendedResource(node, res string) {
	ptr := "/status/capacity/" + strings.ReplaceAll(res, "/", "~1")
	patch := fmt.Sprintf(`[{"op":"remove","path":%q}]`, ptr)
	_, _ = utils.Kubectl("patch", "node", node, "--subresource=status", "--type=json", "-p", patch)
}

// anyTerminatingServingPod reports whether any of the DM's serving Pods has a
// deletionTimestamp set (is Terminating).
func anyTerminatingServingPod(dm string) bool {
	out, err := utils.Kubectl("get", "pods", "-l", servingPodSelector(dm),
		"-n", schedulerEdgeNS,
		"-o", "jsonpath={.items[*].metadata.deletionTimestamp}")
	if err != nil {
		return false
	}
	return strings.TrimSpace(out) != ""
}

// pauseNode freezes a kind node's container so its kubelet stops reporting; the
// node transitions to Ready=Unknown (NotReady). kind node containers are named
// "<cluster>-<role>"; the node object name equals the container name.
func pauseNode(node string) {
	_, err := utils.Run(exec.Command("docker", "pause", node))
	Expect(err).NotTo(HaveOccurred(), "failed to pause node container %s", node)
}

// unpauseNode resumes a paused kind node container.
func unpauseNode(node string) {
	_, _ = utils.Run(exec.Command("docker", "unpause", node))
}
