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
	"sort"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/maks3201/decision-model-operator/test/utils"
)

const (
	// fleetNS holds the whole fleet so teardown is a single namespace delete.
	fleetNS = "dmo-e2e-fleet"
	// fleetSize is the number of DecisionModels created at once.
	fleetSize = 500
	// fleetBudget is the configured --max-concurrent-rollouts for the run.
	fleetBudget = 5
)

// This container is a fleet-scale stress test: it creates fleetSize (500)
// DecisionModels at once against an operator configured with
// --max-concurrent-rollouts=fleetBudget (5), and measures the controller under that
// load. It is labelled nightly+fleet (far too slow and large for a per-PR run) and
// runs in its own namespace with a single manager replica.
//
// No real model loads (method). Every DecisionModel requests a memory amount far
// larger than any node can allocate (fleetHugeMem). The budget still admits exactly
// fleetBudget of them — each records a candidateRevision and creates a prefetch Job
// and candidate Deployment — but those Pods are unschedulable, so no weights are
// ever pulled and no Ollaya model is ever loaded; the other (fleetSize-fleetBudget)
// DecisionModels stay in phase Pending (RolloutQueued) behind the budget. This
// exercises exactly the fleet-scale hot path we care about — reconcile throughput,
// the budget's List+count on every queued DM's requeue, status writes, and API-server
// load for 500 objects — with a bounded, near-zero real resource footprint.
//
// What it deliberately does NOT measure: real prefetch (pull) throughput, serving-Pod
// scheduling/readiness-gate timing, model warmup, or promotion latency (nothing ever
// becomes model-ready). Those are covered by the lifecycle, HA and chaos suites. It
// also does not measure cross-leader behaviour (single manager replica); the HA
// suites cover leader election.
//
// Reported numbers (printed to the Ginkgo log, not asserted as thresholds, so a
// slower CI runner does not flake the gate): reconcile latency
// (controller_runtime_reconcile_time_seconds), total reconciles and errors,
// manager resident memory (process_resident_memory_bytes), and API requests per
// minute (rest_client_requests_total over the window). The only hard assertion is
// the budget invariant: never more than fleetBudget DecisionModels carry a
// candidateRevision at once.
var _ = Describe("fleet stress: 500 DecisionModels under a rollout budget",
	Label("nightly", "fleet"), Ordered, func() {

		// fleetHugeMem is larger than any CI node's allocatable memory, so every
		// candidate's prefetch Job Pod and serving Pod stay Pending (unschedulable):
		// the operator records the admission and runs its state machine, but nothing
		// is ever pulled or loaded.
		const fleetHugeMem = "1000Gi"

		BeforeAll(func() {
			if testInstall == "helm" {
				Skip("helm job runs lifecycle specs only")
			}
			installAndDeploy()

			By("configuring the rollout budget on the running manager")
			patchManagerArgs("--max-concurrent-rollouts=" + strconv.Itoa(fleetBudget))

			_, _ = utils.Kubectl("create", "ns", fleetNS)
		})

		AfterAll(func() {
			// A single namespace delete reaps all 500 DecisionModels and their owned
			// objects (OwnerReferences); give it room on a loaded node.
			_, _ = utils.Kubectl("delete", "ns", fleetNS, "--ignore-not-found", "--timeout=5m")
			_, _ = utils.Kubectl("delete", "clusterrolebinding", metricsReaderSA, "--ignore-not-found")
			_, _ = utils.Kubectl("delete", "serviceaccount", metricsReaderSA, "-n", operatorNamespace, "--ignore-not-found")
			undeploy()
		})

		AfterEach(func() {
			if CurrentSpecReport().Failed() {
				dumpManagerDiag()
				phases, _ := utils.Kubectl("get", "decisionmodel", "-n", fleetNS,
					"-o", "jsonpath={range .items[*]}{.status.phase}{'\\n'}{end}")
				_, _ = fmt.Fprintf(GinkgoWriter, "phase histogram:\n%s\n", histogram(phases))
			}
		})

		It("holds the budget and stays responsive across 500 DecisionModels", func() {
			By(fmt.Sprintf("creating %d DecisionModels with an unschedulable memory request", fleetSize))
			createStart := time.Now()
			for i := 0; i < fleetSize; i++ {
				applyYAML(fmt.Sprintf(`apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata: {name: fleet-%04d, namespace: %s}
spec:
  engine: ollaya
  model: %s
  device: %s
  replicas: 1
  resources: {requests: {cpu: 10m, memory: %s}, limits: {memory: %s}}
`, i, fleetNS, testModel, testDevice, fleetHugeMem, fleetHugeMem))
			}
			_, _ = fmt.Fprintf(GinkgoWriter, "applied %d DecisionModels in %s\n", fleetSize, time.Since(createStart))

			By("the budget admits exactly the limit; the rest queue — and the limit is never exceeded")
			// Poll the admitted count repeatedly: it must converge to fleetBudget and
			// must NEVER exceed it at any observation (the budget invariant under load).
			var maxAdmitted int
			Eventually(func(g Gomega) {
				admitted := countWithCandidate()
				if admitted > maxAdmitted {
					maxAdmitted = admitted
				}
				g.Expect(admitted).To(BeNumerically("<=", fleetBudget),
					"never more than the budget may carry a candidate at once (saw %d)", admitted)
				g.Expect(admitted).To(Equal(fleetBudget),
					"the budget should fill up to exactly its limit")
			}, 10*time.Minute, 10*time.Second).Should(Succeed())
			Expect(maxAdmitted).To(BeNumerically("<=", fleetBudget),
				"the admitted count must never have exceeded the budget during the run")

			By("the remaining DecisionModels are queued (Pending), none are Ready (no real loads)")
			Eventually(func(g Gomega) {
				pending := countPhase("Pending")
				ready := countPhase("Ready")
				g.Expect(pending).To(BeNumerically(">=", fleetSize-fleetBudget-5),
					"the bulk of the fleet should be queued behind the budget (pending=%d)", pending)
				g.Expect(ready).To(Equal(0), "nothing can become Ready — the method loads no model")
			}, 10*time.Minute, 10*time.Second).Should(Succeed())

			By("measuring the controller under load (reported, not thresholded)")
			// Take two metric snapshots a fixed window apart to derive rates.
			const window = 60 * time.Second
			m1 := scrapeManagerMetrics()
			time.Sleep(window)
			m2 := scrapeManagerMetrics()

			reconciles := m2.counter("controller_runtime_reconcile_total") -
				m1.counter("controller_runtime_reconcile_total")
			reconcileErrs := m2.reconcileErrors() - m1.reconcileErrors()
			apiReqs := m2.counter("rest_client_requests_total") - m1.counter("rest_client_requests_total")
			reconcileP50, reconcileP90, reconcileP99 := m2.reconcileLatencyQuantiles()

			_, _ = fmt.Fprintf(GinkgoWriter, `
=== fleet stress report (%d DecisionModels, budget %d, %s window) ===
reconciles in window      : %.0f  (%.1f/min)
reconcile errors in window: %.0f
API requests in window    : %.0f  (%.1f/min)
reconcile latency seconds : p50=%.4f p90=%.4f p99=%.4f
manager resident memory   : %.1f MiB
goroutines                : %.0f
go heap in use            : %.1f MiB
===============================================================
`,
				fleetSize, fleetBudget, window,
				reconciles, reconciles/window.Minutes(),
				reconcileErrs,
				apiReqs, apiReqs/window.Minutes(),
				reconcileP50, reconcileP90, reconcileP99,
				m2.gauge("process_resident_memory_bytes")/(1024*1024),
				m2.gauge("go_goroutines"),
				m2.gauge("go_memstats_heap_inuse_bytes")/(1024*1024),
			)

			By("the manager is still live and the budget invariant still holds after the window")
			Expect(countWithCandidate()).To(BeNumerically("<=", fleetBudget),
				"the budget invariant must still hold after the measurement window")
			Expect(reconcileErrs).To(BeNumerically("<", reconciles),
				"reconcile errors must not dominate (a superlinear error storm would show here)")

			// Superlinear hint: with the fleet steady (budget full, rest queued), a
			// queued DM re-decides on the phase-2 cheap cached path every ~10s, so the
			// reconcile rate should be on the order of fleetSize/10s, not fleetSize^2.
			// We only log the per-DM rate; a human reads the trend across runs.
			_, _ = fmt.Fprintf(GinkgoWriter, "reconciles per DM per minute: %.2f\n",
				reconciles/window.Minutes()/float64(fleetSize))
		})
	})

// countWithCandidate returns how many DecisionModels in the fleet namespace
// currently carry a status.candidateRevision.hash (i.e. are admitted rollouts).
func countWithCandidate() int {
	out, _ := utils.Kubectl("get", "decisionmodel", "-n", fleetNS,
		"-o", "jsonpath={range .items[*]}{.status.candidateRevision.hash}{'\\n'}{end}")
	n := 0
	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) != "" {
			n++
		}
	}
	return n
}

// countPhase returns how many DecisionModels in the fleet namespace are in the
// given status.phase.
func countPhase(phase string) int {
	out, _ := utils.Kubectl("get", "decisionmodel", "-n", fleetNS,
		"-o", "jsonpath={range .items[*]}{.status.phase}{'\\n'}{end}")
	n := 0
	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) == phase {
			n++
		}
	}
	return n
}

// histogram summarises newline-separated phase strings as "phase=count" lines for
// a failure dump.
func histogram(lines string) string {
	counts := map[string]int{}
	for _, l := range strings.Split(lines, "\n") {
		l = strings.TrimSpace(l)
		if l == "" {
			l = "(empty)"
		}
		counts[l]++
	}
	var sb strings.Builder
	for k, v := range counts {
		fmt.Fprintf(&sb, "%s=%d\n", k, v)
	}
	return sb.String()
}

// metricsSample is one parsed Prometheus text scrape of the manager's /metrics.
// Lines are kept verbatim and parsed on demand by the helpers below (counters,
// gauges, and the reconcile-latency histogram), so a metric the manager does not
// export yet simply reads as zero rather than failing the scrape.
type metricsSample struct {
	lines []string
}

// metricsReaderSA is the throwaway ServiceAccount bound to the metrics-reader
// ClusterRole so a token can read the HTTPS+RBAC-protected /metrics endpoint.
const metricsReaderSA = "fleet-metrics-reader"

// ensureMetricsReader creates (idempotently) a ServiceAccount in the operator
// namespace bound to the metrics-reader ClusterRole, so scrapeManagerMetrics can
// mint a token that is authorized for the /metrics nonResourceURL. The ClusterRole
// name is resolved at runtime because the install carries an install-specific
// prefix (kustomize "decision-model-operator-metrics-reader", helm its own).
func ensureMetricsReader() {
	_, _ = utils.Kubectl("create", "serviceaccount", metricsReaderSA, "-n", operatorNamespace)
	role := metricsReaderClusterRole()
	Expect(role).NotTo(BeEmpty(), "metrics-reader ClusterRole not found")
	_, _ = utils.Kubectl("create", "clusterrolebinding", metricsReaderSA,
		"--clusterrole="+role,
		"--serviceaccount="+operatorNamespace+":"+metricsReaderSA)
}

// metricsReaderClusterRole returns the install's metrics-reader ClusterRole name
// (its base name ends with "metrics-reader" under any install prefix).
func metricsReaderClusterRole() string {
	out, _ := utils.Kubectl("get", "clusterrole", "-o",
		"jsonpath={range .items[*]}{.metadata.name}{'\\n'}{end}")
	for _, name := range strings.Split(out, "\n") {
		name = strings.TrimSpace(name)
		if strings.HasSuffix(name, "metrics-reader") {
			return name
		}
	}
	return ""
}

// metricsServiceName resolves the controller-manager metrics Service name, which
// carries the install prefix (kustomize/helm), by its control-plane label and the
// "metrics-service" name suffix.
func metricsServiceName() string {
	out, _ := utils.Kubectl("get", "svc", "-n", operatorNamespace,
		"-l", "control-plane=controller-manager",
		"-o", "jsonpath={range .items[*]}{.metadata.name}{'\\n'}{end}")
	for _, name := range strings.Split(out, "\n") {
		name = strings.TrimSpace(name)
		if strings.HasSuffix(name, "metrics-service") {
			return name
		}
	}
	return strings.TrimSpace(strings.Split(out, "\n")[0])
}

// scrapeManagerMetrics reads the manager's /metrics endpoint (HTTPS :8443,
// authn/authz-protected) by port-forwarding the metrics Service and curling it with
// a bearer token for a metrics-reader ServiceAccount. It returns the parsed sample.
func scrapeManagerMetrics() metricsSample {
	ensureMetricsReader()
	svc := metricsServiceName()
	Expect(svc).NotTo(BeEmpty(), "metrics Service not found")

	var body string
	Eventually(func(g Gomega) {
		token, err := utils.Kubectl("create", "token", metricsReaderSA, "-n", operatorNamespace, "--duration=10m")
		g.Expect(err).NotTo(HaveOccurred(), "failed to mint a metrics-reader token")
		token = strings.TrimSpace(token)

		stop := make(chan struct{})
		local := portForward(svc, operatorNamespace, 8443, stop)
		defer close(stop)

		out, cerr := utils.Run(exec.Command("curl", "-sS", "-k", "--max-time", "30",
			"-H", "Authorization: Bearer "+token,
			fmt.Sprintf("https://%s/metrics", local)))
		g.Expect(cerr).NotTo(HaveOccurred())
		g.Expect(out).To(ContainSubstring("controller_runtime_reconcile_total"),
			"metrics scrape did not return controller-runtime series (got: %.120s)", out)
		body = out
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
	return metricsSample{lines: strings.Split(body, "\n")}
}

// counter sums every series whose name matches (labels ignored), the Prometheus
// convention for a counter family. Returns 0 when the family is absent.
func (m metricsSample) counter(name string) float64 {
	var total float64
	for _, l := range m.lines {
		if strings.HasPrefix(l, "#") {
			continue
		}
		if strings.HasPrefix(l, name+" ") || strings.HasPrefix(l, name+"{") {
			total += lastField(l)
		}
	}
	return total
}

// gauge returns the first series value for a (process/go) gauge with no labels.
func (m metricsSample) gauge(name string) float64 {
	for _, l := range m.lines {
		if strings.HasPrefix(l, "#") {
			continue
		}
		if strings.HasPrefix(l, name+" ") {
			return lastField(l)
		}
	}
	return 0
}

// reconcileErrors sums controller_runtime_reconcile_total for series labelled
// result="error".
func (m metricsSample) reconcileErrors() float64 {
	var total float64
	for _, l := range m.lines {
		if strings.HasPrefix(l, "controller_runtime_reconcile_total{") && strings.Contains(l, `result="error"`) {
			total += lastField(l)
		}
	}
	return total
}

// latencyBucket is one histogram bucket (upper bound + cumulative count).
type latencyBucket struct {
	le    float64
	count float64
}

// reconcileLatencyQuantiles derives p50/p90/p99 of
// controller_runtime_reconcile_time_seconds from its histogram buckets (summed
// across controllers). It returns the upper bound of the first bucket whose
// cumulative count reaches the quantile; good enough for a reported-not-asserted
// figure.
func (m metricsSample) reconcileLatencyQuantiles() (p50, p90, p99 float64) {
	var buckets []latencyBucket
	var inf float64
	for _, l := range m.lines {
		if !strings.HasPrefix(l, "controller_runtime_reconcile_time_seconds_bucket{") {
			continue
		}
		le := labelValue(l, "le")
		v := lastField(l)
		if le == "+Inf" {
			inf += v
			continue
		}
		f, err := strconv.ParseFloat(le, 64)
		if err != nil {
			continue
		}
		// Sum buckets with the same le across controllers.
		found := false
		for i := range buckets {
			if buckets[i].le == f {
				buckets[i].count += v
				found = true
				break
			}
		}
		if !found {
			buckets = append(buckets, latencyBucket{le: f, count: v})
		}
	}
	if len(buckets) == 0 {
		return 0, 0, 0
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].le < buckets[j].le })
	total := inf
	if total == 0 {
		total = buckets[len(buckets)-1].count
	}
	q := func(p float64) float64 {
		target := p * total
		for _, b := range buckets {
			if b.count >= target {
				return b.le
			}
		}
		return buckets[len(buckets)-1].le
	}
	return q(0.50), q(0.90), q(0.99)
}

// lastField parses the trailing whitespace-separated field of a Prometheus text
// line as a float (the sample value).
func lastField(line string) float64 {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return 0
	}
	v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
	if err != nil {
		return 0
	}
	return v
}

// labelValue extracts the value of a label from a Prometheus series line, e.g.
// labelValue(`x{le="0.1"} 3`, "le") == "0.1".
func labelValue(line, label string) string {
	key := label + `="`
	i := strings.Index(line, key)
	if i < 0 {
		return ""
	}
	rest := line[i+len(key):]
	j := strings.IndexByte(rest, '"')
	if j < 0 {
		return ""
	}
	return rest[:j]
}
