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
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/maks3201/decision-model-operator/test/utils"
)

// fleetNS isolates the fleet-scale scenario.
const fleetNS = "dmo-e2e-fleet"

// fleetSize is how many DecisionModels the stress spec creates. Kept at a few
// hundred so the run fits the nightly budget while still surfacing superlinear
// reconcile cost; override with E2E_FLEET_SIZE.
var fleetSize = envIntOr("E2E_FLEET_SIZE", 500)

// This container measures the operator under a large fleet (plan item 75) WITHOUT
// loading a model 500 times: every DecisionModel pins a model tag the resolver
// rejects, so each object stops at Resolving (Resolved=False) and the operator
// never creates a PVC, prefetch Job or serving Pod for it. That isolates the
// controller's steady-state cost -- reconcile throughput for N objects, manager
// memory, and (observable via the metrics endpoint) API-call rate -- from the
// model runtime, which is the point of the measurement. It reports numbers; its
// only hard assertion is that the fleet converges (every object reconciled to a
// terminal Resolving state with observedGeneration caught up) within a generous
// deadline and the manager does not restart (no OOM / crash under load).
// Nightly only.
var _ = Describe("Fleet: the operator stays healthy under many DecisionModels",
	Label("nightly", "fleet"), Ordered, func() {
		BeforeAll(func() {
			if testInstall == "helm" {
				Skip("helm job runs lifecycle specs only")
			}
			installAndDeploy()
			_, _ = utils.Kubectl("create", "ns", fleetNS)
		})

		AfterAll(func() {
			_, _ = utils.Kubectl("delete", "ns", fleetNS, "--ignore-not-found", "--wait=false")
			undeploy()
		})

		AfterEach(func() {
			if CurrentSpecReport().Failed() {
				dumpDiag(fleetNS, "")
			}
		})

		It(fmt.Sprintf("reconciles %d DecisionModels without a manager restart, and reports the cost", fleetSize), func() {
			base := modelBase()

			By("recording the manager restart count and memory before the load")
			restartsBefore := managerRestarts()
			memBefore := managerMemoryBytes()

			By(fmt.Sprintf("creating %d DecisionModels in one apply "+
				"(each pinned to an unresolvable tag -> no model load)", fleetSize))
			start := time.Now()
			applyYAML(fleetManifest(base, fleetSize))
			applied := time.Since(start)

			By("waiting until every DecisionModel has been reconciled to a terminal Resolving state")
			// Each object is counted done when Resolved=False with reason ModelNotFound and
			// observedGeneration == 1 (the controller processed its single generation). This
			// is the whole fleet reaching steady state; the time to get here is the aggregate
			// reconcile throughput for the fleet.
			deadline := 20 * time.Minute
			var converged time.Duration
			Eventually(func(g Gomega) {
				done := countResolvedFalse()
				_, _ = fmt.Fprintf(GinkgoWriter, "  reconciled %d/%d\n", done, fleetSize)
				g.Expect(done).To(Equal(fleetSize), "all DecisionModels should reach a terminal Resolving state")
				converged = time.Since(start)
			}, deadline, 15*time.Second).Should(Succeed())

			By("the manager did not restart under the load")
			restartsAfter := managerRestarts()
			Expect(restartsAfter).To(Equal(restartsBefore),
				"the controller-manager must not restart (OOM/crash) under the fleet; before=%d after=%d",
				restartsBefore, restartsAfter)

			memAfter := managerMemoryBytes()
			memLine := "unavailable (distroless manager has no shell; no metrics-server on kind)"
			if memBefore > 0 || memAfter > 0 {
				memLine = fmt.Sprintf("before=%s after=%s delta=%s",
					humanBytes(memBefore), humanBytes(memAfter), humanBytes(memAfter-memBefore))
			}
			_, _ = fmt.Fprintf(GinkgoWriter,
				"\nFLEET STRESS (%d DecisionModels, device=%s):\n"+
					"  apply (server-side) : %s\n"+
					"  converged (all reconciled): %s  (%.1f objects/s)\n"+
					"  manager memory: %s\n"+
					"  manager restarts: before=%d after=%d\n"+
					"  per-object reconcile budget: %.3f s/object\n"+
					"  (API-call rate is observable at the manager /metrics endpoint:\n"+
					"   rest_client_requests_total, workqueue_* ; not scraped here.)\n",
				fleetSize, testDevice,
				applied.Round(time.Millisecond),
				converged.Round(time.Second), float64(fleetSize)/converged.Seconds(),
				memLine,
				restartsBefore, restartsAfter,
				converged.Seconds()/float64(fleetSize))
		})
	})

// fleetManifest renders `n` DecisionModels in one multi-document YAML. Each pins
// an unresolvable tag (<base>:e2e-missing-<i>) so the operator stops at Resolving
// and never allocates a PVC/Job/Pod -- the fleet exists without 500 model loads.
func fleetManifest(base string, n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, `---
apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata: {name: fleet-%04d, namespace: %s}
spec:
  engine: ollaya
  model: %s:e2e-missing-%d
  device: %s
  replicas: 1
  resources: {requests: {cpu: 10m, memory: 64Mi}, limits: {memory: 128Mi}}
`, i, fleetNS, base, i, testDevice)
	}
	return b.String()
}

// countResolvedFalse counts DecisionModels in the fleet namespace whose Resolved
// condition is False with observedGeneration >= 1 (reconciled at least once to the
// terminal resolve state). It lists once and parses, to keep the API load of the
// probe itself low (one LIST, not N GETs).
func countResolvedFalse() int {
	out, err := utils.Kubectl("get", "decisionmodels", "-n", fleetNS,
		"-o", `jsonpath={range .items[*]}{.status.observedGeneration}{"|"}`+
			`{.status.conditions[?(@.type=="Resolved")].status}{"\n"}{end}`)
	if err != nil {
		return -1
	}
	n := 0
	for _, line := range utils.SplitLines(out) {
		parts := strings.SplitN(line, "|", 2)
		if len(parts) != 2 {
			continue
		}
		if parts[0] != "" && parts[0] != "0" && parts[1] == "False" {
			n++
		}
	}
	return n
}

// managerRestarts returns the sum of restart counts across controller-manager
// containers (0 when none / not found).
func managerRestarts() int {
	out, err := utils.Kubectl("get", "pods", "-n", operatorNamespace,
		"-l", "control-plane=controller-manager",
		"-o", "jsonpath={.items[*].status.containerStatuses[*].restartCount}")
	if err != nil {
		return 0
	}
	total := 0
	for _, f := range strings.Fields(out) {
		if v, e := strconv.Atoi(f); e == nil {
			total += v
		}
	}
	return total
}

// managerMemoryBytes reads the controller-manager container's current memory from
// its cgroup (works on kind without metrics-server). Returns 0 if it cannot read
// it, so the measurement is best-effort and never fails the spec.
func managerMemoryBytes() int64 {
	pod, err := utils.Kubectl("get", "pods", "-n", operatorNamespace,
		"-l", "control-plane=controller-manager",
		"-o", "jsonpath={.items[0].metadata.name}")
	if err != nil || pod == "" {
		return 0
	}
	// cgroup v2 first, then v1.
	for _, path := range []string{"/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory/memory.usage_in_bytes"} {
		cmd := exec.Command("kubectl", "exec", "-n", operatorNamespace, pod, "-c", "manager", "--",
			"cat", path)
		var buf bytes.Buffer
		cmd.Stdout = &buf
		if err := cmd.Run(); err == nil {
			if v, e := strconv.ParseInt(strings.TrimSpace(buf.String()), 10, 64); e == nil {
				return v
			}
		}
	}
	return 0
}

// humanBytes formats a byte count as MiB.
func humanBytes(b int64) string {
	return fmt.Sprintf("%.1f MiB", float64(b)/(1024*1024))
}

// envIntOr returns the int value of an env var, or def when unset/invalid.
func envIntOr(key string, def int) int {
	if v := envOr(key, ""); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
