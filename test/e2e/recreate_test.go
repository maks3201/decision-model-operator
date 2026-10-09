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
	"math"
	"os/exec"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/maks3201/decision-model-operator/test/utils"
)

// recreateNS isolates the multi-node co-location and Recreate scenarios.
const recreateNS = "dmo-e2e-recreate"

// This container needs a kind cluster with more than one node (the
// workflow creates it from hack/kind-recreate.yaml). It covers three things the
// single-node shards cannot show:
//
//   - RWO co-location on a real multi-node cluster: the operator pins every
//     replica to the one node holding the ReadWriteOnce model-store volume,
//     rather than the single-node layout co-locating them for free.
//   - The Recreate strategy succeeds where BlueGreen cannot: when the candidate
//     cannot be scheduled alongside the stable (modelling one GPU), BlueGreen
//     (both revisions at once) times out in Starting while Recreate stops the
//     stable first, so the candidate fits and is promoted.
//   - A Recreate candidate that fails the model-ready gate scales the stable back
//     up (StableRestored) and the stable serves again (RolledBack).
//
// It is labelled "recreate" so it runs only on its own multi-node shard, and
// "nightly" so the per-PR label filter (!nightly) never selects it on a
// single-node cluster where it could not schedule the way it expects.
var _ = Describe("Rollout: RWO co-location and the Recreate strategy", Label("recreate", "nightly"), Ordered, func() {
	// dm is the large-request DecisionModel the Recreate specs roll out; its
	// per-Pod CPU request fills most of node A so a candidate cannot be scheduled
	// alongside the stable. coloDM is a separate small-request DecisionModel used
	// only by the co-location spec, where two or three replicas must fit on one
	// node together.
	const dm = "recreate-router"
	const coloDM = "colocation-router"

	// nodeA is the node the stable and candidate are pinned to (its
	// decisionmodel.io/e2e-node=a label, from hack/kind-recreate.yaml). Both
	// revisions target the same node so a per-Pod CPU request that fits only once
	// on it makes the candidate unschedulable while the stable runs.
	const nodeA = "a"

	// podCPU is the per-serving-Pod CPU request, computed in BeforeAll so that two
	// serving Pods cannot both fit on node A (2*podCPU > allocatable) but one can
	// (podCPU <= allocatable). This is what makes a BlueGreen candidate
	// unschedulable next to the stable regardless of the runner's core count.
	var podCPU string

	BeforeAll(func() {
		if testInstall == "helm" {
			Skip("helm job runs lifecycle specs only")
		}
		installAndDeploy()
		_, _ = utils.Kubectl("create", "ns", recreateNS)

		By("sizing a per-Pod CPU request so two serving Pods cannot share node A")
		podCPU = perPodCPURequest(nodeA)
		_, _ = fmt.Fprintf(GinkgoWriter, "per-Pod CPU request pinned to node A: %s\n", podCPU)

		By("bringing up a stable revision pinned to node A")
		applyYAML(recreateDM(dm, 1, podCPU, nodeA, ""))
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(recreateNS, "decisionmodel", dm, "{.status.phase}")
		}, 8*time.Minute, 5*time.Second).Should(Equal("Ready"))
	})

	AfterAll(func() {
		_, _ = utils.Kubectl("delete", "ns", recreateNS, "--ignore-not-found")
		undeploy()
	})

	AfterEach(func() { dumpDiag(recreateNS, dm) })

	It("co-locates replicas on the RWO store node and keeps replicas in-place", func() {
		By("bringing up a small-request DecisionModel pinned to node A")
		// A small CPU request so two or three replicas fit on one node together;
		// the Recreate dm uses a large request for the opposite reason, so the two
		// scenarios do not share a DecisionModel.
		applyYAML(recreateDM(coloDM, 1, "250m", nodeA, ""))
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(recreateNS, "decisionmodel", coloDM, "{.status.phase}")
		}, 8*time.Minute, 5*time.Second).Should(Equal("Ready"))
		DeferCleanup(func() {
			_, _ = utils.Kubectl("delete", "decisionmodel", coloDM, "-n", recreateNS, "--ignore-not-found")
		})

		revBefore, err := utils.KubectlJSONPath(recreateNS, "decisionmodel", coloDM, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(revBefore).NotTo(BeEmpty())

		By("scaling to replicas: 2 in place")
		patchReplicas(coloDM, 2)

		By("both replicas become model-ready and are co-located on one node")
		Eventually(func(g Gomega) {
			ready, _ := utils.KubectlJSONPath(recreateNS, "decisionmodel", coloDM, "{.status.replicas.modelReady}")
			g.Expect(ready).To(Equal("2"), "expected 2 model-ready replicas")
			nodes := servingPodNodes(coloDM)
			g.Expect(nodes).To(HaveLen(2), "expected 2 running serving Pods, got %v", nodes)
			g.Expect(nodes[0]).To(Equal(nodes[1]),
				"both replicas should be co-located on one node (got %v)", nodes)
		}, 8*time.Minute, 10*time.Second).Should(Succeed())

		By("the serving Deployment carries the required host pod-affinity, not a CacheNotShareable warning")
		dep := coloDM + "-" + revBefore
		topoKey, err := utils.KubectlJSONPath(recreateNS, "deploy", dep,
			"{.spec.template.spec.affinity.podAffinity."+
				"requiredDuringSchedulingIgnoredDuringExecution[0].topologyKey}")
		Expect(err).NotTo(HaveOccurred())
		Expect(topoKey).To(Equal("kubernetes.io/hostname"),
			"serving Deployment should carry a required host pod-affinity for co-location")
		reason, _ := utils.KubectlJSONPath(recreateNS, "decisionmodel", coloDM,
			"{.status.conditions[?(@.type=='Degraded')].reason}")
		Expect(reason).NotTo(Equal("CacheNotShareable"),
			"co-located replicas must not raise CacheNotShareable")

		By("a Normal ReplicasCoLocated Event was emitted")
		Eventually(func() (string, error) {
			return utils.Kubectl("get", "events", "-n", recreateNS,
				"--field-selector", "reason=ReplicasCoLocated,involvedObject.name="+coloDM,
				"-o", "jsonpath={.items[*].reason}")
		}, 2*time.Minute, 5*time.Second).Should(ContainSubstring("ReplicasCoLocated"))

		By("scaling to replicas: 3 in place does not start a new revision")
		patchReplicas(coloDM, 3)
		Consistently(func() (string, error) {
			return utils.KubectlJSONPath(recreateNS, "decisionmodel", coloDM, "{.status.stableRevision.hash}")
		}, 30*time.Second, 10*time.Second).Should(Equal(revBefore),
			"a replicas change is in-place and must not roll a new revision")
		Expect(candidateRevision(coloDM)).To(BeEmpty(), "no candidate revision for an in-place replicas change")

		By("deleting the co-location DecisionModel to free node capacity for the Recreate specs")
		_, _ = utils.Kubectl("delete", "decisionmodel", coloDM, "-n", recreateNS, "--ignore-not-found")
	})

	It("promotes a Recreate candidate that BlueGreen could not schedule, and reports the downtime", func() {
		stableBefore, err := utils.KubectlJSONPath(recreateNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stableBefore).NotTo(BeEmpty())

		By("rolling out a candidate next to the stable under the default BlueGreen strategy")
		// A changed memory limit forces a new revision (resources are part of the
		// revision hash) pinned to the same node with the same large CPU request, so
		// the candidate cannot be scheduled while the stable still runs (both would
		// need node A, which fits only one). Both limits stay above the model size so
		// neither Pod is OOM-killed; only the CPU request governs scheduling.
		applyYAML(recreateDM(dm, 1, podCPU, nodeA, "5Gi"))
		var candHash string
		Eventually(func(g Gomega) {
			h := candidateRevision(dm)
			g.Expect(h).NotTo(BeEmpty(), "a candidate revision should be recorded")
			g.Expect(h).NotTo(Equal(stableBefore))
			candHash = h
		}, 5*time.Minute, 5*time.Second).Should(Succeed())

		By("BlueGreen cannot promote: the candidate Pod stays Pending (Unschedulable) and the stable keeps serving")
		Eventually(func(g Gomega) {
			g.Expect(candidatePodUnschedulable(dm, candHash)).To(BeTrue(),
				"the BlueGreen candidate should be Unschedulable next to the stable")
		}, 5*time.Minute, 10*time.Second).Should(Succeed())
		Expect(serviceRevision(recreateNS, dm)).To(Equal(stableBefore),
			"BlueGreen must keep the Service on the stable while the candidate cannot start")
		Expect(utils.KubectlJSONPath(recreateNS, "decisionmodel", dm, "{.status.stableStoppedForRevision}")).
			To(Equal(""), "BlueGreen must not stop the stable")

		By("switching the same candidate to the Recreate strategy")
		// Strategy is not part of the revision hash: the candidate is the same
		// revision, now allowed to stop the stable first.
		_, err = utils.Kubectl("patch", "decisionmodel", dm, "-n", recreateNS, "--type=merge",
			"-p", `{"spec":{"rollout":{"strategy":"Recreate"}}}`)
		Expect(err).NotTo(HaveOccurred())

		By("the operator stops the stable (StableStopped) to free node A for the candidate")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(recreateNS, "decisionmodel", dm, "{.status.stableStoppedForRevision}")
		}, 5*time.Minute, 5*time.Second).Should(Equal(candHash),
			"Recreate should record the stable as stopped for the candidate")
		Eventually(func() (string, error) {
			return utils.Kubectl("get", "events", "-n", recreateNS,
				"--field-selector", "reason=StableStopped", "-o", "jsonpath={.items[*].reason}")
		}, 2*time.Minute, 5*time.Second).Should(ContainSubstring("StableStopped"))

		By("measuring the serving downtime until the candidate is promoted")
		downtime := measureDowntime(dm, func() bool {
			phase, _ := utils.KubectlJSONPath(recreateNS, "decisionmodel", dm, "{.status.phase}")
			return phase == "Ready" && serviceRevision(recreateNS, dm) == candHash
		}, 12*time.Minute)
		_, _ = fmt.Fprintf(GinkgoWriter,
			"Recreate promotion downtime window (no ready endpoints): %s\n", downtime)

		By("the candidate is promoted to the stable and the stopped marker is cleared")
		stableNow, err := utils.KubectlJSONPath(recreateNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stableNow).To(Equal(candHash), "the Recreate candidate should become the stable")
		Expect(serviceRevision(recreateNS, dm)).To(Equal(candHash), "the Service should route to the promoted candidate")
		Expect(utils.KubectlJSONPath(recreateNS, "decisionmodel", dm, "{.status.stableStoppedForRevision}")).
			To(Equal(""), "the stopped marker should be cleared on promotion")
	})

	It("restores the stable when a Recreate candidate fails the model-ready gate, and reports the downtime", func() {
		stableBefore, err := utils.KubectlJSONPath(recreateNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stableBefore).NotTo(BeEmpty())

		By("rolling out a Recreate candidate that can never become model-ready")
		// device: cuda on a CPU cluster adds an nvidia.com/gpu request the kind
		// nodes cannot satisfy, so the candidate Pod stays Pending and never
		// reaches the model-ready gate. A short starting timeout bounds the wait so
		// the stable is restored quickly. The prefetch Job (no GPU request) still
		// caches the model, so the rollout reaches the stop-the-stable step before
		// the candidate fails — exactly the Recreate rollback path.
		applyYAML(recreateFailingDM(dm, podCPU, nodeA))
		var candHash string
		Eventually(func(g Gomega) {
			h := candidateRevision(dm)
			g.Expect(h).NotTo(BeEmpty())
			g.Expect(h).NotTo(Equal(stableBefore))
			candHash = h
		}, 5*time.Minute, 5*time.Second).Should(Succeed())

		By("the stable is stopped for the candidate (StableStopped)")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(recreateNS, "decisionmodel", dm, "{.status.stableStoppedForRevision}")
		}, 6*time.Minute, 5*time.Second).Should(Equal(candHash))

		downtimeStart := time.Now()

		By("the candidate fails its gate, the stable is scaled back up and restored (RolledBack)")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(recreateNS, "decisionmodel", dm, "{.status.phase}")
		}, 12*time.Minute, 10*time.Second).Should(Equal("RolledBack"),
			"a Recreate candidate that never becomes ready should roll back to the stable")

		By("a StableRestored Event was emitted and the stopped marker is cleared")
		Eventually(func() (string, error) {
			return utils.Kubectl("get", "events", "-n", recreateNS,
				"--field-selector", "reason=StableRestored", "-o", "jsonpath={.items[*].reason}")
		}, 2*time.Minute, 5*time.Second).Should(ContainSubstring("StableRestored"))
		Expect(utils.KubectlJSONPath(recreateNS, "decisionmodel", dm, "{.status.stableStoppedForRevision}")).
			To(Equal(""), "the stopped marker should be cleared after the stable is restored")
		failed, _ := utils.KubectlJSONPath(recreateNS, "decisionmodel", dm, "{.status.failedRevision.hash}")
		Expect(failed).To(Equal(candHash), "the failed revision should be the candidate")
		stableNow, _ := utils.KubectlJSONPath(recreateNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(stableNow).To(Equal(stableBefore), "the original stable should be restored unchanged")

		By("the restored stable serves /v1/systemone again")
		// After a Recreate rollback the stable Deployment is scaled back up and its
		// Pod reloads the model; once its readiness probe passes it is added to the
		// Service endpoints and answers again. (We assert serving via the endpoint
		// rather than status.replicas.modelReady: on the RolledBack path the
		// model-ready gate is re-probed from Pod events, so the endpoint is the
		// contract the task names — "the stable answers /v1/systemone again".)
		Eventually(func(g Gomega) {
			eps, _ := utils.Kubectl("get", "endpoints", dm, "-n", recreateNS,
				"-o", "jsonpath={.subsets[*].addresses[*].ip}")
			g.Expect(strings.Fields(eps)).NotTo(BeEmpty(), "the restored stable should have a ready endpoint")
		}, 12*time.Minute, 10*time.Second).Should(Succeed())
		downtime := time.Since(downtimeStart)
		_, _ = fmt.Fprintf(GinkgoWriter,
			"Recreate rollback downtime window (stable stopped until it serves again): ~%s\n",
			downtime.Round(time.Second))

		stop := make(chan struct{})
		local := portForward(dm, recreateNS, 11435, stop)
		defer close(stop)
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("curl", "-sS", "--max-time", "60", "-X", "POST",
				fmt.Sprintf("http://%s/v1/systemone", local),
				"-H", "Content-Type: application/json",
				"--data-binary", `{"model":"`+testModel+`","keep_alive":-1,"state":{},"questions":{}}`))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(BeEmpty(), "the restored stable should answer /v1/systemone")
		}, 3*time.Minute, 5*time.Second).Should(Succeed())
	})
})

// ---- helpers for the multi-node co-location / Recreate specs ----

// recreateDM renders a DecisionModel pinned to one node via spec.scheduling with
// a large per-Pod CPU request. memLimit sets the memory limit; a change between
// revisions forces a new revision hash (resources are part of the hash) without
// changing the CPU request that governs scheduling, so the candidate stays
// pinned to the same node and the "fits only once" constraint is unchanged.
// nodeVal is the decisionmodel.io/e2e-node label of the target node.
func recreateDM(name string, replicas int, cpu, nodeVal, memLimit string) string {
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
  resources:
    requests: {cpu: %s, memory: 1Gi}
    limits: {memory: %s}
  scheduling:
    nodeSelector: {decisionmodel.io/e2e-node: %s}
`, name, recreateNS, testModel, testDevice, replicas, cpu, memLimit, nodeVal)
}

// recreateFailingDM renders a Recreate candidate that can never become
// model-ready: device cuda on a CPU cluster adds an unsatisfiable nvidia.com/gpu
// request. A short starting timeout bounds the Recreate rollback.
func recreateFailingDM(name, cpu, nodeVal string) string {
	return fmt.Sprintf(`apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata: {name: %s, namespace: %s}
spec:
  engine: ollaya
  model: %s
  device: cuda
  replicas: 1
  resources:
    requests: {cpu: %s, memory: 1Gi}
    limits: {memory: 4Gi}
  scheduling:
    nodeSelector: {decisionmodel.io/e2e-node: %s}
  rollout:
    strategy: Recreate
    timeouts: {starting: 3m}
`, name, recreateNS, testModel, cpu, nodeVal)
}

// patchReplicas sets spec.replicas with a merge patch.
func patchReplicas(name string, replicas int) {
	_, err := utils.Kubectl("patch", "decisionmodel", name, "-n", recreateNS, "--type=merge",
		"-p", fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas))
	Expect(err).NotTo(HaveOccurred())
}

// candidateRevision returns the DM's candidate revision hash (empty if none).
func candidateRevision(name string) string {
	h, _ := utils.KubectlJSONPath(recreateNS, "decisionmodel", name, "{.status.candidateRevision.hash}")
	return h
}

// servingPodNodes returns the node names of the DM's running serving Pods.
func servingPodNodes(name string) []string {
	out, err := utils.Kubectl("get", "pods", "-l", servingPodSelector(name),
		"-n", recreateNS, "--field-selector=status.phase=Running",
		"-o", "jsonpath={.items[*].spec.nodeName}")
	Expect(err).NotTo(HaveOccurred())
	return strings.Fields(out)
}

// candidatePodUnschedulable reports whether the candidate revision has a Pod that
// is Pending with an Unschedulable scheduling condition (the BlueGreen candidate
// that cannot fit next to the stable).
func candidatePodUnschedulable(name, rev string) bool {
	pods, err := utils.Kubectl("get", "pods",
		"-l", "decisionmodel.io/name="+name+",decisionmodel.io/revision="+rev,
		"-n", recreateNS, "-o", "jsonpath={.items[*].metadata.name}")
	if err != nil {
		return false
	}
	for _, p := range strings.Fields(pods) {
		phase, _ := utils.KubectlJSONPath(recreateNS, "pod", p, "{.status.phase}")
		reason, _ := utils.KubectlJSONPath(recreateNS, "pod", p,
			"{.status.conditions[?(@.type=='PodScheduled')].reason}")
		if phase == "Pending" && reason == "Unschedulable" {
			return true
		}
	}
	return false
}

// measureDowntime polls until done() is true (or the deadline), returning how
// long the DM reported no model-ready serving endpoint during the window. It
// samples status.replicas.modelReady; the returned duration is the time spent at
// 0 ready replicas, i.e. the documented Recreate traffic gap.
func measureDowntime(name string, done func() bool, deadline time.Duration) time.Duration {
	start := time.Now()
	var down time.Duration
	last := start
	for time.Since(start) < deadline {
		now := time.Now()
		ready, _ := utils.KubectlJSONPath(recreateNS, "decisionmodel", name, "{.status.replicas.modelReady}")
		if ready == "" || ready == "0" {
			down += now.Sub(last)
		}
		last = now
		if done() {
			break
		}
		time.Sleep(5 * time.Second)
	}
	return down.Round(time.Second)
}

// perPodCPURequest reads node A's allocatable CPU and returns a per-Pod CPU
// request (in milli-CPU, e.g. "1800m") sized so two serving Pods cannot both fit
// on the node (2*req > allocatable) but one can (req <= allocatable). This makes a
// BlueGreen candidate unschedulable next to the stable independent of the runner's
// core count.
func perPodCPURequest(nodeVal string) string {
	node, err := utils.Kubectl("get", "nodes",
		"-l", "decisionmodel.io/e2e-node="+nodeVal,
		"-o", "jsonpath={.items[0].metadata.name}")
	Expect(err).NotTo(HaveOccurred())
	Expect(node).NotTo(BeEmpty(), "node with label decisionmodel.io/e2e-node=%s not found", nodeVal)

	alloc, err := utils.KubectlJSONPath("", "node", node, "{.status.allocatable.cpu}")
	Expect(err).NotTo(HaveOccurred())
	milli := cpuToMilli(alloc)
	Expect(milli).To(BeNumerically(">", 0), "could not read allocatable CPU %q", alloc)

	// 60% of allocatable: one Pod fits, two (120%) do not. Floor at 500m so the
	// request stays meaningful on a tiny node; the suite needs >=~900m allocatable.
	req := int(math.Round(float64(milli) * 0.6))
	if req < 500 {
		req = 500
	}
	return strconv.Itoa(req) + "m"
}

// cpuToMilli parses a Kubernetes CPU quantity ("2", "1500m") into milli-CPU.
func cpuToMilli(q string) int {
	q = strings.TrimSpace(q)
	if q == "" {
		return 0
	}
	if strings.HasSuffix(q, "m") {
		n, err := strconv.Atoi(strings.TrimSuffix(q, "m"))
		if err != nil {
			return 0
		}
		return n
	}
	f, err := strconv.ParseFloat(q, 64)
	if err != nil {
		return 0
	}
	return int(f * 1000)
}
