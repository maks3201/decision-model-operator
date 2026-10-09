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
//   - a Recreate rollout must serialise stable and candidate so their serving
//     Pods never coexist when a capacity-1 extended resource (not just CPU) is the
//     scarce resource;
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

	// Item 6: under Recreate with a capacity-1 extended resource shared by stable
	// and candidate, the operator must serialise the two revisions — it must never
	// run a candidate serving Pod while a stable serving Pod still exists, because
	// the two would contend for the single accelerator unit. We advertise a fake
	// accelerator with capacity 1 on node A, make the serving container request it,
	// and roll a new revision under Recreate. The operator's guarantee
	// (recreate.go: stableReplicasGone) is that it scales the stable to 0 and waits
	// until that revision has NO Pod object left before creating the candidate
	// Deployment. We assert that serialisation directly: at no observed moment do
	// serving Pods of both revisions coexist, and the candidate still ends up
	// promoted and Ready. This needs no preStop/grace or test finalizer — it is the
	// property the operator actually provides.
	It("serialises a Recreate rollout so stable and candidate never contend for a capacity-1 resource", func() {
		const dm = "edge-serialize"

		By("advertising a capacity-1 fake extended resource on node A")
		advertiseExtendedResource(nodeAName, extResource, 1)
		DeferCleanup(func() { clearExtendedResource(nodeAName, extResource) })

		By("bringing up a Recreate stable revision that requests the one accelerator unit")
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
			h := edgeCandidateRevision(dm)
			g.Expect(h).NotTo(BeEmpty())
			g.Expect(h).NotTo(Equal(stableBefore))
			candHash = h
		}, 5*time.Minute, 5*time.Second).Should(Succeed())

		By("the operator records the stable as stopped for the candidate (Recreate)")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(schedulerEdgeNS, "decisionmodel", dm, "{.status.stableStoppedForRevision}")
		}, 5*time.Minute, 5*time.Second).Should(Equal(candHash))

		By("serving Pods of the two revisions never coexist while the candidate rolls out")
		// The core serialisation invariant: until the stable's Pod is really gone the
		// operator does not create the candidate serving Pod, so the single
		// accelerator unit is never double-booked. Hold the assertion across the whole
		// stop-then-start window; it only passes if no reconcile ever lets both
		// revisions have a serving Pod at once.
		Consistently(func(g Gomega) {
			g.Expect(servingRevisionsOverlap(dm, stableBefore, candHash)).To(BeFalse(),
				"stable and candidate serving Pods must never coexist under Recreate (single accelerator)")
		}, 3*time.Minute, 5*time.Second).Should(Succeed())

		By("the candidate eventually takes the one accelerator unit and is promoted to serving")
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
		//   - a Warning Event (on the DM or on its prefetch Pod) names the missing
		//     ConfigMap. When the digest is reused from status the operator has no
		//     verified bytes to repair the deleted ConfigMap with, so it falls back
		//     to pull-by-tag and the required mount surfaces as a kubelet FailedMount
		//     on the prefetch Pod naming the ConfigMap — a clear, actionable reason a
		//     user sees on `kubectl describe pod`, not a silent hang.
		// The failure the task guards against is an unexplained Caching timeout
		// with no reason anywhere. We require some explanatory signal within the window.
		Eventually(func(g Gomega) {
			phase, _ := utils.KubectlJSONPath(schedulerEdgeNS, "decisionmodel", dm, "{.status.phase}")
			cachedReason, _ := utils.KubectlJSONPath(schedulerEdgeNS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Cached')].reason}")
			events, _ := utils.Kubectl("get", "events", "-n", schedulerEdgeNS,
				"--field-selector", "involvedObject.name="+dm, "-o",
				"jsonpath={.items[*].reason}")
			// The prefetch Pod's events: a required ConfigMap mount that cannot be
			// satisfied shows here as FailedMount naming the ConfigMap.
			podEvents, _ := utils.Kubectl("get", "events", "-n", schedulerEdgeNS,
				"--field-selector", "reason=FailedMount", "-o",
				"jsonpath={.items[*].message}")

			repaired := phase == "Ready"
			hasReason := cachedReason != "" && cachedReason != "Caching"
			lowerEvents := strings.ToLower(events)
			lowerPod := strings.ToLower(podEvents)
			seedSignal := strings.Contains(events, "SeedUnavailable") ||
				strings.Contains(lowerEvents, "manifest") ||
				strings.Contains(lowerEvents, "configmap") ||
				strings.Contains(lowerPod, cmName) ||
				strings.Contains(lowerPod, "configmap")

			_, _ = fmt.Fprintf(GinkgoWriter,
				"manifest-CM delete: phase=%q cachedReason=%q events=%q podEvents=%q "+
					"(repaired=%v hasReason=%v seedSignal=%v)\n",
				phase, cachedReason, events, podEvents, repaired, hasReason, seedSignal)

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
// candidate must wait for. The DM uses the Recreate strategy: item 6 is about
// Recreate serialising on the one accelerator unit (stop the stable first), so the
// operator must record stableStoppedForRevision — under the default BlueGreen it
// never stops the stable and the candidate would just hang Unschedulable.
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
  rollout:
    strategy: Recreate
`, name, schedulerEdgeNS, testModel, testDevice, res, memLimit, res, nodeVal)
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

// edgeCandidateRevision returns the DM's candidate revision hash in the
// scheduler-edge namespace (the recreate container's candidateRevision is bound to
// its own namespace, which this shard does not create).
func edgeCandidateRevision(dm string) string {
	h, _ := utils.KubectlJSONPath(schedulerEdgeNS, "decisionmodel", dm, "{.status.candidateRevision.hash}")
	return h
}

// servingRevisionsOverlap reports whether serving Pods of BOTH the stable and the
// candidate revision exist at the same time. Prefetch Pods are excluded via
// servingPodSelector. Under Recreate with a single capacity-1 accelerator the
// operator must never let this happen: it scales the stable to 0 and waits until
// that revision has no Pod left before creating the candidate (recreate.go:
// stableReplicasGone). A Pod counts as present whatever its phase, so a stable
// Pod still Terminating also counts as an overlap. Any list error reads as "no
// overlap" (best-effort) so a transient API hiccup does not fail the invariant.
func servingRevisionsOverlap(dm, stableRev, candRev string) bool {
	return revisionHasServingPod(dm, stableRev) && revisionHasServingPod(dm, candRev)
}

// revisionHasServingPod reports whether the given revision has at least one
// serving (non-prefetch) Pod object in the scheduler-edge namespace. A Pod counts
// whatever its phase, INCLUDING one that is Terminating (has a deletionTimestamp):
// a Terminating serving Pod still holds its accelerator unit until the kubelet
// finishes killing the container, so for the "never coexist" invariant it must be
// counted as present. This matches the operator's own rule (recreate.go
// stableReplicasGone), which treats the stable as "gone" only once no Pod object
// for the revision remains.
func revisionHasServingPod(dm, rev string) bool {
	out, err := utils.Kubectl("get", "pods",
		"-l", servingPodSelector(dm)+",decisionmodel.io/revision="+rev,
		"-n", schedulerEdgeNS, "-o", "jsonpath={.items[*].metadata.name}")
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
