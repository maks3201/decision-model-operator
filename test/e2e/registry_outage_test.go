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
	"os"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/maks3201/decision-model-operator/test/utils"
)

// registryOutageNS holds the DecisionModel under test for the registry-outage
// scenarios. The in-cluster mirror lives in mirrorNS (reused from the mirror spec
// via deployMirror) so the registry can be broken independently of the DM.
const registryOutageNS = "dmo-e2e-registry-outage"

// registryOutageDM is the DecisionModel reused across the scenarios (every laya:en
// Pod holds the model, so the specs run sequentially to bound node memory).
const registryOutageDM = "registry-outage-router"

// This container drives registry outages that the HTTP-status chaos specs do not
// cover: a DNS failure (the registry host does not resolve), a TCP reset
// (connection accepted then abruptly closed), an outage that strikes AFTER a
// successful Resolve but as the prefetch starts, and an outage DURING stable store
// recovery. Each asserts the documented reason/condition, that the stable keeps
// serving throughout, and that it recovers once the fault clears.
//
// It runs through the in-cluster mirror (so the registry can be broken) and is
// laya-specific (the mirror image bakes in laya:en). Nightly only and slow (model
// loads), so it is labelled "registry-outage" and "nightly".
var _ = Describe("Registry outages: DNS, reset, mistimed faults", Label("registry-outage", "nightly"), Ordered, func() {
	const dm = registryOutageDM

	BeforeAll(func() {
		if testInstall == "helm" {
			Skip("helm job runs lifecycle specs only")
		}
		if os.Getenv("E2E_MIRROR") == "" {
			Skip("set E2E_MIRROR to build the mirror image and run the registry-outage spec")
		}
		if !modelIsLaya() {
			Skip("laya-specific: the registry mirror image serves laya:en")
		}
		installAndDeploy()
		deployMirror()
		By("pointing the operator at the mirror (and allow-listing a bogus host for the DNS spec)")
		patchManagerArgs(
			"--ollaya-registry=http://"+mirrorHost,
			// Allow both the real mirror host and the non-resolving host the DNS
			// scenario switches to, so the DNS failure is a resolve/transport error
			// and NOT an allow-list rejection.
			"--allowed-registries="+mirrorHost+","+bogusRegistryHost,
			"--allow-insecure-registries",
		)

		_, _ = utils.Kubectl("create", "ns", registryOutageNS)
		By("bringing up a stable revision through the mirror")
		applyYAML(fmt.Sprintf(`apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata: {name: %s, namespace: %s}
spec:
  engine: ollaya
  model: laya:en
  device: %s
  replicas: 1
  resources: {requests: {cpu: 250m, memory: 1Gi}, limits: {memory: 4Gi}}
`, dm, registryOutageNS, testDevice))
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(registryOutageNS, "decisionmodel", dm, "{.status.phase}")
		}, 8*time.Minute, 5*time.Second).Should(Equal("Ready"))
	})

	AfterAll(func() {
		clearMirrorFault()
		// Restore the operator's registry to the mirror in case a spec left it on the
		// bogus host (its own cleanup should do this, but be safe for later shards).
		patchManagerArgs(
			"--ollaya-registry=http://"+mirrorHost,
			"--allowed-registries="+mirrorHost+","+bogusRegistryHost,
			"--allow-insecure-registries",
		)
		_, _ = utils.Kubectl("delete", "ns", registryOutageNS, "--ignore-not-found")
		_, _ = utils.Kubectl("delete", "ns", mirrorNS, "--ignore-not-found")
		undeploy()
	})

	AfterEach(func() {
		dumpDiag(registryOutageNS, dm)
		clearMirrorFault()
	})

	// Item 62 (DNS failure): the registry host does not resolve. Point the operator
	// at a bogus host and force a fresh resolve; the DNS lookup fails as a transport
	// error, so a candidate resolve is a transient ResolveFailed, the stable keeps
	// serving, and restoring the real host recovers.
	It("keeps the stable serving when the registry hostname does not resolve (DNS failure)", func() {
		stableBefore := mustStable()

		By("re-pointing the operator at a non-resolving registry host (this rolls the manager)")
		// patchManagerArgs restarts the manager. Right after that restart the
		// model-ready prober briefly reports NoModelReadyPods for the single stable
		// Pod before it re-establishes the gate — a restart artifact, NOT a
		// registry-outage effect. We let that flap settle (manager rolled out inside
		// patchManagerArgs, then the DM back to Ready) BEFORE triggering the failing
		// resolve, so the strict assertion below reflects only the DNS outage. The
		// stable does not use the registry, so it returns to Ready even with the
		// bogus host set, as long as no new resolve is in flight.
		patchManagerArgs(
			"--ollaya-registry=http://"+bogusRegistryHost,
			"--allowed-registries="+mirrorHost+","+bogusRegistryHost,
			"--allow-insecure-registries",
		)
		By("waiting for the manager-restart flap to settle: the stable is Ready again")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(registryOutageNS, "decisionmodel", dm, "{.status.phase}")
		}, 4*time.Minute, 5*time.Second).Should(Equal("Ready"),
			"the stable should return to Ready after the manager restart, before any registry fault")

		By("forcing a fresh resolve against the non-resolving host (the actual DNS fault)")
		patchModel("laya:multilingual")

		By("Resolved=False/ResolveFailed (DNS is a transport error, not ModelNotFound), stable serves")
		Eventually(func(g Gomega) {
			reason, _ := utils.KubectlJSONPath(registryOutageNS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Resolved')].reason}")
			g.Expect(reason).To(Equal("ResolveFailed"),
				"a DNS failure should be a transient ResolveFailed, not ModelNotFound")
		}, 4*time.Minute, 5*time.Second).Should(Succeed())
		assertStableKeepsServing(stableBefore, "Ready")

		By("restoring the real registry host and model recovers Resolved=True, stable intact")
		patchManagerArgs(
			"--ollaya-registry=http://"+mirrorHost,
			"--allowed-registries="+mirrorHost+","+bogusRegistryHost,
			"--allow-insecure-registries",
		)
		patchModel("laya:en")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(registryOutageNS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Resolved')].status}")
		}, 5*time.Minute, 5*time.Second).Should(Equal("True"))
		Expect(mustStable()).To(Equal(stableBefore), "a DNS blip must not roll the stable")
	})

	// Item 62 (TCP reset): the registry accepts the connection then abruptly closes
	// it (MIRROR_FAULT=reset), so the client sees a transport reset/EOF rather than
	// an HTTP status. This must be classified as transient (ResolveFailed), the
	// stable keeps serving, and clearing the fault recovers.
	It("treats a registry TCP reset during Resolving as transient (stable keeps serving)", func() {
		stableBefore := mustStable()

		By("faulting the registry with a TCP reset and forcing a new resolve")
		setMirrorFault("reset")
		patchModel("laya:multilingual")

		By("Resolved=False/ResolveFailed (a transport reset is retryable/transient), stable serves")
		Eventually(func(g Gomega) {
			reason, _ := utils.KubectlJSONPath(registryOutageNS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Resolved')].reason}")
			g.Expect(reason).To(Equal("ResolveFailed"),
				"a TCP reset should be a transient ResolveFailed, not ModelNotFound")
		}, 4*time.Minute, 5*time.Second).Should(Succeed())
		assertStableKeepsServing(stableBefore, "Ready")

		By("clearing the fault and restoring the model recovers Resolved=True")
		clearMirrorFault()
		patchModel("laya:en")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(registryOutageNS, "decisionmodel", dm,
				"{.status.conditions[?(@.type=='Resolved')].status}")
		}, 5*time.Minute, 5*time.Second).Should(Equal("True"))
		Expect(mustStable()).To(Equal(stableBefore))
	})

	// Item 62 (outage after Resolve, before prefetch): a new revision resolves while
	// the registry is healthy (digest recorded), then the registry faults as the
	// prefetch Job starts. The DecisionModel must wait in Caching (the Job retries),
	// not Failed/RolledBack; once the registry recovers the prefetch completes and
	// the candidate promotes.
	It("waits in Caching when the registry fails after Resolve but during prefetch, then completes", func() {
		stableBefore := mustStable()

		By("resolving a new revision while healthy, then faulting the registry as prefetch starts")
		// Bump CPU to force a new revision; resolve succeeds first (registry healthy).
		patchResourcesCPU("600m")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(registryOutageNS, "decisionmodel", dm, "{.status.candidateRevision.hash}")
		}, 4*time.Minute, 2*time.Second).ShouldNot(BeEmpty())
		// Now the digest is recorded; fault the registry so the prefetch pull fails and retries.
		setMirrorFault("503")

		By("the DecisionModel waits in Caching (not Failed/RolledBack) while the Job retries")
		Consistently(func(g Gomega) {
			phase, _ := utils.KubectlJSONPath(registryOutageNS, "decisionmodel", dm, "{.status.phase}")
			g.Expect(phase).NotTo(BeElementOf("Failed", "RolledBack"),
				"an outage during prefetch must keep the rollout waiting, not fail it")
		}, 40*time.Second, 5*time.Second).Should(Succeed())
		assertStableKeepsServing(stableBefore, "Caching")

		By("clearing the fault lets the prefetch complete and the candidate promote")
		clearMirrorFault()
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(registryOutageNS, "decisionmodel", dm, "{.status.phase}")
		}, 12*time.Minute, 10*time.Second).Should(Equal("Ready"))
		Expect(mustStable()).NotTo(Equal(stableBefore), "the new revision should promote after recovery")
	})

	// Item 62 (outage during stable store recovery): the stable store PVC is deleted
	// and its Pod removed so the operator must re-prefetch (store recovery) through
	// the registry; the registry faults during that recovery. The operator must not
	// fail or roll the stable on the transient fault, and must recover (reach Ready
	// on the same stable revision) once the fault clears.
	It("recovers stable store re-prefetch across a registry outage without failing or rolling back", func() {
		stableBefore := mustStable()
		pvc := dm + "-store-" + stableBefore

		By("faulting the registry, then deleting the stable store PVC and its Pod to force recovery")
		setMirrorFault("503")
		_, _ = utils.Kubectl("delete", "pvc", pvc, "-n", registryOutageNS, "--wait=false")
		_, _ = utils.Kubectl("delete", "pods", "-l",
			"decisionmodel.io/name="+dm+",decisionmodel.io/revision="+stableBefore, "-n", registryOutageNS,
			"--grace-period=0", "--force")

		By("the operator does not fail or roll back the stable while the registry is down")
		// Store recovery re-runs the prefetch; with the registry faulted the pull
		// retries. The DM may go Degraded/Caching, but must not land in a terminal
		// Failed/RolledBack on a transient registry fault.
		Consistently(func(g Gomega) {
			phase, _ := utils.KubectlJSONPath(registryOutageNS, "decisionmodel", dm, "{.status.phase}")
			g.Expect(phase).NotTo(BeElementOf("Failed", "RolledBack"),
				"a transient registry outage during store recovery must not fail/roll back the stable")
			stableNow, _ := utils.KubectlJSONPath(registryOutageNS, "decisionmodel", dm, "{.status.stableRevision.hash}")
			g.Expect(stableNow).To(Equal(stableBefore),
				"the stable revision identity must not change during a store-recovery outage")
		}, 40*time.Second, 5*time.Second).Should(Succeed())

		By("clearing the fault lets the store re-prefetch complete and the stable serve again")
		clearMirrorFault()
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(registryOutageNS, "decisionmodel", dm, "{.status.phase}")
		}, 12*time.Minute, 10*time.Second).Should(Equal("Ready"),
			"once the registry recovers the stable store should re-prefetch and serve again")
		Expect(mustStable()).To(Equal(stableBefore), "the recovered store is the same stable revision")
	})
})

// ---- helpers local to the registry-outage container ----

// bogusRegistryHost is an in-cluster name that does not resolve (no such Service),
// used to drive the DNS-failure scenario. It is on the operator's allow-list so the
// failure is a DNS/transport error, not an allow-list rejection.
const bogusRegistryHost = "no-such-registry.dmo-e2e-registry-outage.svc"

// mustStable returns the DM's stable revision hash, failing the spec if empty.
func mustStable() string {
	h, err := utils.KubectlJSONPath(registryOutageNS, "decisionmodel", registryOutageDM, "{.status.stableRevision.hash}")
	Expect(err).NotTo(HaveOccurred())
	Expect(h).NotTo(BeEmpty())
	return h
}

// patchModel sets spec.model with a merge patch.
func patchModel(model string) {
	_, err := utils.Kubectl("patch", "decisionmodel", registryOutageDM, "-n", registryOutageNS, "--type=merge",
		"-p", fmt.Sprintf(`{"spec":{"model":%q}}`, model))
	Expect(err).NotTo(HaveOccurred())
}

// patchResourcesCPU sets the serving CPU request (forcing a new revision hash).
func patchResourcesCPU(cpu string) {
	_, err := utils.Kubectl("patch", "decisionmodel", registryOutageDM, "-n", registryOutageNS, "--type=merge",
		"-p", fmt.Sprintf(`{"spec":{"resources":{"requests":{"cpu":%q,"memory":"1Gi"},"limits":{"memory":"4Gi"}}}}`, cpu))
	Expect(err).NotTo(HaveOccurred())
}

// assertStableKeepsServing asserts the strict property the registry-outage specs
// exist for: a transient, candidate-side registry fault must not disturb the
// already-serving stable. The stable does not need the registry, so during the
// fault its revision identity is unchanged and its Service keeps a ready endpoint.
//
// wantPhase is the phase the DM must hold throughout: "Ready" when the fault hits
// before a candidate is admitted (Resolve fails, no rollout in progress), or the
// rollout stage (e.g. "Caching") once a candidate is admitted, because the phase
// reports the rollout stage. Callers MUST have let any manager-restart flap settle
// (manager rolled out AND the DM back to Ready) BEFORE injecting the fault, so the
// phase check reflects the registry fault only (see the DNS spec).
func assertStableKeepsServing(stable, wantPhase string) {
	Consistently(func(g Gomega) {
		phase, _ := utils.KubectlJSONPath(registryOutageNS, "decisionmodel", registryOutageDM, "{.status.phase}")
		g.Expect(phase).To(Equal(wantPhase),
			"a transient candidate-side registry outage must not move the DecisionModel off "+wantPhase)
		stableNow, _ := utils.KubectlJSONPath(registryOutageNS, "decisionmodel", registryOutageDM,
			"{.status.stableRevision.hash}")
		g.Expect(stableNow).To(Equal(stable),
			"the stable revision must not change during a transient candidate-side outage")
		eps, _ := utils.Kubectl("get", "endpoints", registryOutageDM, "-n", registryOutageNS,
			"-o", "jsonpath={.subsets[*].addresses[*].ip}")
		g.Expect(strings.Fields(eps)).NotTo(BeEmpty(),
			"the stable Service must keep a ready endpoint during a transient registry outage")
	}, 30*time.Second, 5*time.Second).Should(Succeed())
}
