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
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/maks3201/decision-model-operator/test/utils"
)

// haNS isolates the HA failover scenarios from the other suites.
const haNS = "dmo-e2e-ha"

// haDM is the DecisionModel (and Service) name used by the HA scenarios.
const haDM = "ha-router"

// leaseName is the leader-election Lease the manager creates. controller-runtime
// derives the Lease name from LeaderElectionID ("4aba9cc6.io" in cmd/main.go), so
// the Lease object carries exactly that name in the operator's own namespace.
const leaseName = "4aba9cc6.io"

// leaseTakeoverBudget is how long we allow a standby replica to acquire the Lease
// after the holder Pod is deleted. The manager runs with LeaderElectionReleaseOnCancel
// off (cmd/main.go), so a deleted leader does NOT hand the Lease over gracefully:
// the standby must wait out the controller-runtime default LeaseDuration (15s) plus
// a RetryPeriod (2s) before it can acquire. 150s is a generous ceiling for a loaded
// CI node; the test quotes the time actually observed (locally ~17s).
const leaseTakeoverBudget = 150 * time.Second

// This container proves leader-election HA works end to end, not merely that the
// flag is set: it runs the manager with two replicas and, in each reachable rollout
// phase, deletes the Pod that currently holds the Lease and asserts the surviving
// replica takes over and finishes the rollout with the same outcome and no
// duplicated side effects. Labelled "nightly" (it needs two manager replicas plus
// a candidate model load, which would blow the per-PR time budget).
//
// Phase coverage vs. the operator's observable surface:
//   - Evaluating          — reachable; killing the leader here exercises the
//     handover across the (unobservable) Promoting step:
//     PhasePromoting is reserved and never set (constants.go),
//     so "failover during Promoting" is tested as "failover
//     during Evaluating, then assert the promotion still
//     lands after the new leader takes over".
//   - AwaitingPromotion   — reachable with promotion: Manual; proves the parked
//     approval is honored exactly once across a leader change.
//   - Stabilizing         — a condition, not a phase (there is no PhaseStabilizing);
//     reached with a long stabilization window and asserted
//     via the Stabilizing condition.
//   - Caching / Starting  — real phases but short and cache-warmed on a reused
//     node, so they cannot be hit reliably without sleeping;
//     instead the Evaluating scenario drives a full fresh
//     rollout (Resolving -> Caching -> Starting -> Evaluating)
//     and asserts the invariants that a Caching/Starting
//     failover would (one prefetch Job per revision, one
//     candidate Deployment, reaches Ready).
//
// Exactly-once is asserted on cluster STATE (one stableRevision transition to the
// candidate, previousRevision == the prior stable, one Deployment/Job per revision,
// the promote annotation cleared once) AND on the Promoted Event as a DELTA: each
// scenario records the Promoted count right before it rolls its candidate and asserts
// exactly one more afterwards. (Counting the absolute number would be wrong because
// BeforeAll's initial stable is itself a promotion; see the per-scenario comments.)
var _ = Describe("HA: leader-election failover", Label("nightly", "ha"), Ordered, func() {
	const dm = haDM
	var haVerified []string

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

		_, _ = utils.Kubectl("create", "ns", haNS)

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
`, dm, haNS, testModel, testDevice))
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(haNS, "decisionmodel", dm, "{.status.phase}")
		}, 8*time.Minute, 5*time.Second).Should(Equal("Ready"))

		haVerified = learnLabels(dm, haNS)
	})

	AfterAll(func() {
		_, _ = utils.Kubectl("delete", "ns", haNS, "--ignore-not-found")
		// Restore a single manager replica so a reused cluster (local kind) is left
		// as the suite found it; the kustomize manifest default is 1.
		scaleManager(1)
		undeploy()
	})

	AfterEach(func() {
		dumpDiag(haNS, dm)
		dumpManagerDiag()
	})

	It("completes an eval-gated promotion after the leader is killed mid-Evaluating", func() {
		stableBefore, err := utils.KubectlJSONPath(haNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stableBefore).NotTo(BeEmpty())

		By("rolling out an eval-gated candidate the model passes")
		applyConfigMap(haNS, "ha-eval", "cases.jsonl", datasetJSONL(haVerified))
		promotedBefore := eventReasonCount("Promoted")
		patchRollout(dm, haNS, rolloutPatch{
			cpu:          "350m",
			datasetCM:    "ha-eval",
			minAccuracy:  "0.5",
			evaluatingTO: "20m",
		})

		By("waiting until the candidate is Evaluating (or already past it)")
		var candHash string
		Eventually(func(g Gomega) {
			phase, _ := utils.KubectlJSONPath(haNS, "decisionmodel", dm, "{.status.phase}")
			h, _ := utils.KubectlJSONPath(haNS, "decisionmodel", dm, "{.status.candidateRevision.hash}")
			g.Expect(h).NotTo(BeEmpty(), "a candidate revision should be recorded")
			candHash = h
			g.Expect(phase).To(BeElementOf("Evaluating", "Ready"),
				"candidate should reach Evaluating (phase was %q)", phase)
		}, 10*time.Minute, 5*time.Second).Should(Succeed())

		By("killing the Lease holder and asserting the standby takes over within the lease budget")
		took := killLeaderAndAwaitTakeover()
		_, _ = fmt.Fprintf(GinkgoWriter, "leader failover (Evaluating) took %s\n", took)

		By("the promotion still lands on the new leader: the candidate becomes stable")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(haNS, "decisionmodel", dm, "{.status.phase}")
		}, 10*time.Minute, 5*time.Second).Should(Equal("Ready"),
			"the rollout must finish Ready after the failover")
		stableNow, err := utils.KubectlJSONPath(haNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stableNow).To(Equal(candHash), "the evaluated candidate should become the stable revision")
		Expect(serviceRevision(haNS, dm)).To(Equal(candHash), "Service should route to the promoted revision")

		By("exactly one stableRevision transition to the candidate (previousRevision is the prior stable)")
		prev, err := utils.KubectlJSONPath(haNS, "decisionmodel", dm, "{.status.previousRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(prev).To(Equal(stableBefore),
			"previousRevision should be the stable that was serving before the candidate")

		By("no duplicate work: one prefetch Job and exactly one Deployment for the promoted revision")
		Expect(jobCountForRevision(haNS, dm, candHash)).To(Equal(1),
			"exactly one prefetch Job should exist for the candidate revision")
		Expect(deploymentCountForRevision(haNS, dm, candHash)).To(Equal(1),
			"exactly one serving Deployment should carry the promoted revision (no duplicate candidate)")
		// Right after promotion the previous revision's Deployment is intentionally
		// kept (out of the Service) for the stabilization window as the rollback
		// safety net, so up to two Deployments is expected; a leader change must not
		// add a third.
		Expect(len(deploymentRevisions(haNS, dm))).To(BeNumerically("<=", 2),
			"only the promoted and the kept-previous revision Deployments should exist")

		By("exactly one more Promoted Event than before this candidate was rolled out")
		// The failover does not duplicate the promotion: the candidate adds exactly one
		// Promoted Event over the count taken right before patchRollout (the earlier
		// Promoted belongs to BeforeAll's initial stable, not to this candidate).
		dumpTerminalEvents(haNS, dm, "Promoted")
		Expect(eventReasonCount("Promoted")).To(Equal(promotedBefore+1),
			"the candidate's promotion should add exactly one Promoted Event")

		assertObservedGeneration(haNS, dm)
	})

	It("honors a parked manual approval exactly once after the leader is killed mid-AwaitingPromotion", func() {
		stableBefore, err := utils.KubectlJSONPath(haNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stableBefore).NotTo(BeEmpty())

		By("rolling out a Manual-promotion candidate that parks in AwaitingPromotion")
		applyConfigMap(haNS, "ha-manual", "cases.jsonl", datasetJSONL(haVerified))
		promotedBefore := eventReasonCount("Promoted")
		patchRollout(dm, haNS, rolloutPatch{
			cpu:          "450m", // distinct from the previous revision so a new revision is forced
			datasetCM:    "ha-manual",
			minAccuracy:  "0.5",
			evaluatingTO: "20m",
			promotion:    "Manual",
		})

		var candHash string
		Eventually(func(g Gomega) {
			phase, _ := utils.KubectlJSONPath(haNS, "decisionmodel", dm, "{.status.phase}")
			g.Expect(phase).To(Equal("AwaitingPromotion"), "candidate should park awaiting approval")
			h, _ := utils.KubectlJSONPath(haNS, "decisionmodel", dm, "{.status.candidateRevision.hash}")
			g.Expect(h).NotTo(BeEmpty())
			candHash = h
		}, 12*time.Minute, 10*time.Second).Should(Succeed())
		Expect(serviceRevision(haNS, dm)).To(Equal(stableBefore),
			"Service must stay on stable while awaiting promotion")

		By("killing the Lease holder while parked and asserting takeover")
		took := killLeaderAndAwaitTakeover()
		_, _ = fmt.Fprintf(GinkgoWriter, "leader failover (AwaitingPromotion) took %s\n", took)

		By("the parked state survives the failover (still AwaitingPromotion, not self-promoted)")
		Consistently(func() (string, error) {
			return utils.KubectlJSONPath(haNS, "decisionmodel", dm, "{.status.phase}")
		}, 20*time.Second, 5*time.Second).Should(Equal("AwaitingPromotion"),
			"a Manual candidate must not promote itself after a leader change")

		By("approving once on the new leader by the evaluation approvalId")
		approval, err := utils.KubectlJSONPath(haNS, "decisionmodel", dm, "{.status.evaluation.approvalId}")
		Expect(err).NotTo(HaveOccurred())
		if approval == "" {
			approval = candHash
		}
		_, err = utils.Kubectl("annotate", "decisionmodel", dm, "-n", haNS,
			"decisionmodel.io/promote="+approval, "--overwrite")
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(haNS, "decisionmodel", dm, "{.status.phase}")
		}, 5*time.Minute, 5*time.Second).Should(Equal("Ready"),
			"the approved candidate should promote on the new leader")
		stableNow, err := utils.KubectlJSONPath(haNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stableNow).To(Equal(candHash), "the approved candidate should become stable")
		prev, err := utils.KubectlJSONPath(haNS, "decisionmodel", dm, "{.status.previousRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(prev).To(Equal(stableBefore),
			"previousRevision should be the stable that was serving before the candidate")

		By("the approval was consumed exactly once (state): the promote annotation is cleared and not reused")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(haNS, "decisionmodel", dm,
				"{.metadata.annotations.decisionmodel\\.io/promote}")
		}, 2*time.Minute, 5*time.Second).Should(BeEmpty(),
			"the operator should clear the promote annotation after honoring it once")
		By("exactly one more Promoted Event than before this candidate was rolled out")
		dumpTerminalEvents(haNS, dm, "Promoted")
		Expect(eventReasonCount("Promoted")).To(Equal(promotedBefore+1),
			"the approved candidate's promotion should add exactly one Promoted Event")

		assertObservedGeneration(haNS, dm)
	})

	It("finishes the stabilization window after the leader is killed mid-Stabilizing", func() {
		stableBefore, err := utils.KubectlJSONPath(haNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stableBefore).NotTo(BeEmpty())

		By("promoting a new revision with a long stabilization window")
		applyConfigMap(haNS, "ha-stab", "cases.jsonl", datasetJSONL(haVerified))
		// A long window keeps the Stabilizing condition True long enough to kill the
		// leader inside it; the candidate still auto-promotes (no Manual policy).
		_, err = utils.Kubectl("patch", "decisionmodel", dm, "-n", haNS, "--type=merge",
			"-p", `{"spec":{"rollout":{"stabilization":"4m"}}}`)
		Expect(err).NotTo(HaveOccurred())
		promotedBefore := eventReasonCount("Promoted")
		patchRollout(dm, haNS, rolloutPatch{
			cpu:          "550m", // force a fresh revision
			datasetCM:    "ha-stab",
			minAccuracy:  "0.5",
			evaluatingTO: "20m",
			promotion:    "Automatic", // override the Manual policy left by the previous scenario
		})

		var candHash string
		By("waiting until the new revision is promoted and the Stabilizing window is open")
		Eventually(func(g Gomega) {
			h, _ := utils.KubectlJSONPath(haNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
			g.Expect(h).NotTo(BeEmpty())
			candHash = h
			stabilizing, _ := utils.KubectlJSONPath(haNS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Stabilizing')].status}")
			prev, _ := utils.KubectlJSONPath(haNS, "decisionmodel", dm, "{.status.previousRevision.hash}")
			g.Expect(stabilizing).To(Equal("True"), "the Stabilizing condition should be True in the window")
			g.Expect(prev).To(Equal(stableBefore),
				"the previous revision (the prior stable) should be kept during stabilization")
		}, 12*time.Minute, 5*time.Second).Should(Succeed())

		By("killing the Lease holder inside the stabilization window and asserting takeover")
		took := killLeaderAndAwaitTakeover()
		_, _ = fmt.Fprintf(GinkgoWriter, "leader failover (Stabilizing) took %s\n", took)

		By("stabilization completes on the new leader: Stabilizing clears, no post-promotion rollback")
		Eventually(func(g Gomega) {
			stabilizing, _ := utils.KubectlJSONPath(haNS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Stabilizing')].status}")
			g.Expect(stabilizing).NotTo(Equal("True"), "the Stabilizing condition should clear after the window")
			phase, _ := utils.KubectlJSONPath(haNS, "decisionmodel", dm, "{.status.phase}")
			g.Expect(phase).To(Equal("Ready"), "the DM should stay Ready through stabilization")
		}, 8*time.Minute, 5*time.Second).Should(Succeed())

		Expect(eventReasonCount("RolledBackAfterPromotion")).To(Equal(0),
			"a healthy revision must not roll back after promotion because of a leader change")
		stableNow, err := utils.KubectlJSONPath(haNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(stableNow).To(Equal(candHash), "the stabilized revision should remain the stable one")
		Expect(serviceRevision(haNS, dm)).To(Equal(candHash), "Service should stay on the stabilized revision")

		By("exactly one more Promoted Event than before this candidate was rolled out")
		dumpTerminalEvents(haNS, dm, "Promoted")
		Expect(eventReasonCount("Promoted")).To(Equal(promotedBefore+1),
			"the stabilized candidate's promotion should add exactly one Promoted Event")

		assertObservedGeneration(haNS, dm)
	})
})

// ---- HA-specific helpers (leader election, manager scaling, Event counting) ----

// managerDeployment returns the controller-manager Deployment name in
// operatorNamespace, resolved by its label so it works for both the kustomize
// (controller-manager) and helm (templated) names.
func managerDeployment() string {
	out, _ := utils.Kubectl("get", "deploy", "-l", "control-plane=controller-manager",
		"-n", operatorNamespace, "-o", "jsonpath={.items[0].metadata.name}")
	return strings.TrimSpace(out)
}

// scaleManager sets the controller-manager Deployment replica count.
func scaleManager(replicas int) {
	name := managerDeployment()
	Expect(name).NotTo(BeEmpty(), "controller-manager Deployment not found")
	_, err := utils.Kubectl("scale", "deployment", name, "-n", operatorNamespace,
		fmt.Sprintf("--replicas=%d", replicas))
	Expect(err).NotTo(HaveOccurred(), "failed to scale the controller-manager")
}

// managerReadyReplicas returns the manager Deployment's ready replica count.
func managerReadyReplicas() int {
	out, _ := utils.KubectlJSONPath(operatorNamespace, "deployment", managerDeployment(),
		"{.status.readyReplicas}")
	out = strings.TrimSpace(out)
	if out == "" {
		return 0
	}
	var n int
	_, _ = fmt.Sscanf(out, "%d", &n)
	return n
}

// leaseHolder returns the holderIdentity recorded on the leader-election Lease
// (empty if no leader currently holds it).
func leaseHolder() string {
	out, _ := utils.KubectlJSONPath(operatorNamespace, "lease", leaseName, "{.spec.holderIdentity}")
	return strings.TrimSpace(out)
}

// leaderPod maps the current Lease holderIdentity to the manager Pod that holds it.
// controller-runtime's holderIdentity is "<hostname>_<uuid>"; the hostname is the
// Pod name, so the Pod is the holderIdentity up to the first underscore.
func leaderPod() string {
	holder := leaseHolder()
	if holder == "" {
		return ""
	}
	if i := strings.IndexByte(holder, '_'); i >= 0 {
		return holder[:i]
	}
	return holder
}

// killLeaderAndAwaitTakeover deletes the Pod currently holding the Lease and blocks
// until a DIFFERENT identity holds it, returning how long the takeover took. It
// first makes sure two manager replicas are Ready and a standby distinct from the
// leader exists, so the test fails with a clear message if HA was never actually in
// place rather than timing out on the takeover. It fails if no new leader appears
// within leaseTakeoverBudget.
func killLeaderAndAwaitTakeover() time.Duration {
	By("confirming two manager replicas are Ready and a standby exists before the kill")
	Eventually(func(g Gomega) {
		g.Expect(managerReadyReplicas()).To(Equal(2), "both manager replicas must be Ready before a failover")
		g.Expect(leaseHolder()).NotTo(BeEmpty(), "a leader must hold the Lease before the kill")
		pods := managerPods()
		g.Expect(len(pods)).To(BeNumerically(">=", 2), "expected at least two manager Pods")
		g.Expect(pods).To(ContainElement(leaderPod()), "the Lease holder should be one of the manager Pods")
	}, 2*time.Minute, 3*time.Second).Should(Succeed())

	before := leaseHolder()
	pod := leaderPod()
	Expect(pod).NotTo(BeEmpty(), "could not map the Lease holder to a Pod")

	start := time.Now()
	_, err := utils.Kubectl("delete", "pod", pod, "-n", operatorNamespace,
		"--grace-period=0", "--force")
	Expect(err).NotTo(HaveOccurred(), "failed to delete the leader Pod")

	Eventually(func(g Gomega) {
		now := leaseHolder()
		g.Expect(now).NotTo(BeEmpty(), "the Lease has no holder")
		g.Expect(now).NotTo(Equal(before), "a new identity should hold the Lease")
	}, leaseTakeoverBudget, 2*time.Second).Should(Succeed(),
		"a standby replica should take the Lease within %s", leaseTakeoverBudget)
	return time.Since(start)
}

// managerPods returns the names of the manager Pods.
func managerPods() []string {
	out, _ := utils.Kubectl("get", "pods", "-l", "control-plane=controller-manager",
		"-n", operatorNamespace, "-o", "jsonpath={.items[*].metadata.name}")
	return strings.Fields(out)
}

// learnLabels drives the model once per golden state through the Service and
// returns the label it actually answers, so the golden dataset is one the model
// passes regardless of which model is under test (model-agnostic, like the eval
// and rollout suites).
func learnLabels(dm, ns string) []string {
	stop := make(chan struct{})
	local := portForward(dm, ns, 11435, stop)
	defer close(stop)
	verified := make([]string, len(evalStates))
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
	}
	return verified
}

// eventReasonCount counts Events in the HA namespace on the HA DecisionModel with
// the given reason. It counts distinct Event objects (Kubernetes aggregates repeats
// into one object with a count field).
func eventReasonCount(reason string) int {
	out, _ := utils.Kubectl("get", "events", "-n", haNS,
		"--field-selector", "involvedObject.name="+haDM+",reason="+reason,
		"-o", "jsonpath={.items[*].reason}")
	return len(strings.Fields(out))
}

// jobCountForRevision returns the number of prefetch Jobs for a revision.
func jobCountForRevision(ns, name, rev string) int {
	out, _ := utils.Kubectl("get", "job",
		"-l", "decisionmodel.io/name="+name+",decisionmodel.io/revision="+rev,
		"-n", ns, "-o", "jsonpath={.items[*].metadata.name}")
	return len(strings.Fields(out))
}

// deploymentRevisions returns the serving Deployment names for a DecisionModel.
func deploymentRevisions(ns, name string) []string {
	out, _ := utils.Kubectl("get", "deploy", "-l", "decisionmodel.io/name="+name,
		"-n", ns, "-o", "jsonpath={.items[*].metadata.name}")
	return strings.Fields(out)
}

// deploymentCountForRevision returns the number of serving Deployments carrying a
// specific revision label (a duplicate would mean two candidates for one revision).
func deploymentCountForRevision(ns, name, rev string) int {
	out, _ := utils.Kubectl("get", "deploy",
		"-l", "decisionmodel.io/name="+name+",decisionmodel.io/revision="+rev,
		"-n", ns, "-o", "jsonpath={.items[*].metadata.name}")
	return len(strings.Fields(out))
}

// assertObservedGeneration asserts status.observedGeneration has caught up with
// metadata.generation (the reconcile has fully processed the latest spec).
func assertObservedGeneration(ns, name string) {
	Eventually(func(g Gomega) {
		gen, _ := utils.KubectlJSONPath(ns, "decisionmodel", name, "{.metadata.generation}")
		obs, _ := utils.KubectlJSONPath(ns, "decisionmodel", name, "{.status.observedGeneration}")
		g.Expect(obs).To(Equal(gen), "observedGeneration should match generation at the end")
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

// dumpTerminalEvents prints every Event with the given reason for a DecisionModel,
// using the events.k8s.io/v1 schema so each emission's reportingInstance (which
// manager replica emitted it), series.count/deprecatedCount, eventTime and note are
// visible. This is the evidence for whether a leader failover produces one emission
// (one object, possibly count>1) or two distinct emissions from two replicas.
func dumpTerminalEvents(ns, name, reason string) {
	out, err := utils.Kubectl("get", "events.events.k8s.io", "-n", ns,
		"-o", fmt.Sprintf(
			"jsonpath={range .items[?(@.regarding.name=='%s')]}"+
				"{.reason}{'|obj='}{.metadata.name}{'|series.count='}{.series.count}"+
				"{'|deprecatedCount='}{.deprecatedCount}{'|eventTime='}{.eventTime}"+
				"{'|reportingInstance='}{.reportingInstance}{'|note='}{.note}{'\\n'}{end}",
			name))
	_, _ = fmt.Fprintf(GinkgoWriter, "=== %s Events for %s (err=%v) ===\n%s\n", reason, name, err, out)
}

// dumpManagerDiag prints the manager Pods and the Lease on failure, to make a
// failover failure diagnosable from the CI log.
func dumpManagerDiag() {
	if !CurrentSpecReport().Failed() {
		return
	}
	pods, _ := utils.Kubectl("get", "pods", "-l", "control-plane=controller-manager",
		"-n", operatorNamespace, "-o", "wide")
	_, _ = fmt.Fprintf(GinkgoWriter, "manager pods:\n%s\n", pods)
	lease, _ := utils.Kubectl("get", "lease", leaseName, "-n", operatorNamespace, "-o", "yaml")
	_, _ = fmt.Fprintf(GinkgoWriter, "lease:\n%s\n", lease)
}
