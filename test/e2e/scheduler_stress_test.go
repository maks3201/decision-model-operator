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

// slotNS isolates the extended-resource scheduling scenario.
const slotNS = "dmo-e2e-slot"

// slotResource is a fake extended resource advertised on one node (capacity 1) so
// that exactly one serving Pod requesting it can be scheduled at a time. It models
// a single scarce device (e.g. one GPU) without needing real hardware: a BlueGreen
// candidate cannot run alongside the stable (both would need the one slot), while
// Recreate stops the stable first and the candidate then fits.
const slotResource = "decisionmodel.io/e2e-slot"

// This container exercises the single-scarce-resource rollout contract (plan item
// 37) on a single node using a fake extended resource, so it does not need the
// two-node cluster the RWO co-location / Recreate container uses. Nightly only.
var _ = Describe("Scheduling: a single scarce resource gates the rollout",
	Label("nightly", "scheduler"), Ordered, func() {
		const dm = "slot-router"

		var nodeName string

		BeforeAll(func() {
			if testInstall == "helm" {
				Skip("helm job runs lifecycle specs only")
			}
			installAndDeploy()
			_, _ = utils.Kubectl("create", "ns", slotNS)

			By("advertising a fake extended resource (capacity 1) on one node")
			nodeName = advertiseSlot(1)

			By("bringing up a stable revision that holds the one slot")
			applyYAML(slotDM(dm, "4Gi"))
			Eventually(func() (string, error) {
				return utils.KubectlJSONPath(slotNS, "decisionmodel", dm, "{.status.phase}")
			}, 8*time.Minute, 5*time.Second).Should(Equal("Ready"))
		})

		AfterAll(func() {
			_, _ = utils.Kubectl("delete", "ns", slotNS, "--ignore-not-found")
			if nodeName != "" {
				removeSlot(nodeName)
			}
			undeploy()
		})

		AfterEach(func() { dumpDiag(slotNS, dm) })

		It("rolls back a BlueGreen candidate that cannot get the slot, then promotes it under Recreate", func() {
			stableBefore, err := utils.KubectlJSONPath(slotNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
			Expect(err).NotTo(HaveOccurred())
			Expect(stableBefore).NotTo(BeEmpty())

			By("rolling out a candidate (changed memory limit) under the default BlueGreen strategy with a short start timeout")
			// The candidate requests the same single slot the stable holds, so it cannot
			// be scheduled while the stable runs. A short starting timeout bounds the wait.
			applyYAML(slotDMWithTimeout(dm, "5Gi", "3m"))
			var candHash string
			Eventually(func(g Gomega) {
				h, _ := utils.KubectlJSONPath(slotNS, "decisionmodel", dm, "{.status.candidateRevision.hash}")
				g.Expect(h).NotTo(BeEmpty())
				g.Expect(h).NotTo(Equal(stableBefore))
				candHash = h
			}, 5*time.Minute, 5*time.Second).Should(Succeed())

			By("the candidate cannot get the slot: it never runs, and the stable keeps serving")
			// The candidate's own store is prefetched first, so its serving Pod may take
			// a while to appear; once it does it must be Pending/Unschedulable (never
			// Running), because the stable holds the one slot. We assert the invariant
			// that holds throughout: the candidate Pod is never Running and the Service
			// stays on the stable, until BlueGreen gives up at the start timeout.
			Consistently(func(g Gomega) {
				g.Expect(candidatePodRunning(dm, candHash)).To(BeFalse(),
					"the BlueGreen candidate must never run while the stable holds the only slot")
				g.Expect(serviceRevision(slotNS, dm)).To(Equal(stableBefore),
					"BlueGreen must keep the Service on the stable while the candidate cannot start")
			}, 90*time.Second, 10*time.Second).Should(Succeed())

			By("BlueGreen gives up at the start timeout and rolls back (stable served throughout)")
			Eventually(func() (string, error) {
				return utils.KubectlJSONPath(slotNS, "decisionmodel", dm, "{.status.phase}")
			}, 8*time.Minute, 10*time.Second).Should(Equal("RolledBack"),
				"a BlueGreen candidate that never schedules should roll back")
			reason, _ := utils.KubectlJSONPath(slotNS, "decisionmodel", dm, "{.status.failedRevision.reason}")
			Expect(reason).To(Equal("StartTimeout"), "the rollback reason should be StartTimeout")
			Expect(serviceRevision(slotNS, dm)).To(Equal(stableBefore),
				"the stable kept serving through the failed BlueGreen rollout")

			By("rolling out under the Recreate strategy promotes the candidate (stop the stable, free the slot)")
			// A fresh revision (different memory limit, so not the just-failed hash) with
			// strategy Recreate: the controller stops the stable, the one slot frees, and
			// the candidate -- which needs that slot -- is scheduled and promoted. This is
			// the Recreate contract on a single scarce resource.
			applyYAML(slotDMRecreate(dm, "6Gi"))
			var recreateCand string
			Eventually(func(g Gomega) {
				phase, _ := utils.KubectlJSONPath(slotNS, "decisionmodel", dm, "{.status.phase}")
				h, _ := utils.KubectlJSONPath(slotNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
				g.Expect(phase).To(Equal("Ready"), "Recreate should promote the candidate once the slot frees")
				g.Expect(h).NotTo(Equal(stableBefore), "a new revision should be the stable")
				recreateCand = h
			}, 12*time.Minute, 10*time.Second).Should(Succeed())
			Expect(serviceRevision(slotNS, dm)).To(Equal(recreateCand), "the Service should route to the promoted candidate")
			Eventually(func() (string, error) {
				return utils.KubectlJSONPath(slotNS, "decisionmodel", dm, "{.status.stableStoppedForRevision}")
			}, 2*time.Minute, 5*time.Second).Should(Equal(""), "the stopped marker should clear on promotion")
		})
	})

// slotDM renders a DecisionModel that requests one unit of the fake extended
// resource (in limits, where extended resources must be set), with memLimit
// forcing the revision hash when it changes.
func slotDM(name, memLimit string) string {
	return slotManifest(name, memLimit, "", false)
}

// slotDMWithTimeout is slotDM with an explicit starting timeout.
func slotDMWithTimeout(name, memLimit, startTO string) string {
	return slotManifest(name, memLimit, startTO, false)
}

// slotDMRecreate is slotDM with the Recreate strategy (and the same short starting
// timeout) so the stable is stopped before the candidate starts.
func slotDMRecreate(name, memLimit string) string {
	return slotManifest(name, memLimit, "3m", true)
}

func slotManifest(name, memLimit, startTO string, recreate bool) string {
	rollout := ""
	if startTO != "" || recreate {
		rollout = "\n  rollout:"
		if recreate {
			rollout += "\n    strategy: Recreate"
		}
		if startTO != "" {
			rollout += "\n    timeouts: {starting: " + startTO + "}"
		}
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
    requests: {cpu: 250m, memory: 1Gi}
    limits: {memory: %s, %s: "1"}%s
`, name, slotNS, testModel, testDevice, memLimit, slotResource, rollout)
}

// advertiseSlot patches one node's status.capacity to advertise `qty` units of the
// fake extended resource and returns the node name. Extended resources are set on
// the Node status subresource via a JSON patch (the "~1" escapes the "/" in the
// resource name per JSON Pointer).
func advertiseSlot(qty int) string {
	node, err := utils.Kubectl("get", "nodes",
		"-o", "jsonpath={.items[0].metadata.name}")
	Expect(err).NotTo(HaveOccurred())
	Expect(node).NotTo(BeEmpty())
	patch := fmt.Sprintf(`[{"op":"add","path":"/status/capacity/%s","value":"%d"}]`,
		escapeJSONPointer(slotResource), qty)
	_, err = utils.Kubectl("patch", "node", node, "--subresource=status",
		"--type=json", "-p", patch)
	Expect(err).NotTo(HaveOccurred(), "failed to advertise the extended resource on %s", node)
	return node
}

// removeSlot removes the fake extended resource from the node's status.capacity.
func removeSlot(node string) {
	patch := fmt.Sprintf(`[{"op":"remove","path":"/status/capacity/%s"}]`,
		escapeJSONPointer(slotResource))
	_, _ = utils.Kubectl("patch", "node", node, "--subresource=status", "--type=json", "-p", patch)
}

// escapeJSONPointer escapes a resource name for a JSON Pointer path segment
// (RFC 6901): "~" -> "~0", "/" -> "~1".
func escapeJSONPointer(s string) string {
	out := ""
	for _, r := range s {
		switch r {
		case '~':
			out += "~0"
		case '/':
			out += "~1"
		default:
			out += string(r)
		}
	}
	return out
}

// candidatePodRunning reports whether the candidate revision has a serving Pod in
// the Running phase (so it got the scarce resource). False when no Pod exists yet
// or every candidate Pod is Pending — the state a slot-starved BlueGreen candidate
// stays in.
func candidatePodRunning(name, rev string) bool {
	out, err := utils.Kubectl("get", "pods",
		"-l", "decisionmodel.io/name="+name+",decisionmodel.io/revision="+rev,
		"-n", slotNS, "-o", "jsonpath={.items[*].status.phase}")
	if err != nil {
		return false
	}
	for _, phase := range strings.Fields(out) {
		if phase == "Running" {
			return true
		}
	}
	return false
}
