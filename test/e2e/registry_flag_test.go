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

// registryFlagNS holds the DecisionModel for the "a manager registry-flag change
// never rolls a Ready stable" scenario; the in-cluster mirror is reused from the
// mirror spec (deployMirror, in mirrorNS) so the recorded registry is a real,
// reachable base URL and the store-recovery pull can be observed offline.
const registryFlagNS = "dmo-e2e-registry-flag"

// registryFlagDM is the DecisionModel reused across the ordered steps (every
// laya:en Pod holds the model, so the steps run sequentially to bound node memory).
const registryFlagDM = "registry-flag-router"

// This container proves the invariant behind the frozen-stable render and the
// revision-recorded registry: changing the operator's --ollaya-registry flag (set
// it, change it, remove it) never re-renders or rolls a Deployment that is already
// serving a Ready stable, and a store recovery after the flag is gone still pulls
// from the registry the revision was recorded with -- not the current flag.
//
// The regression it guards: before the stable was rendered from its recorded
// identity, a --ollaya-registry change (OLLAYA_REGISTRY is a Deployment env, not a
// field the revision hash covers) re-rendered the stable Deployment, rolled the one
// replica, and the new Pod served the recorded model from a store it could no
// longer reach -> 0 endpoints.
//
// Offline note: the real default registry (https://ollaya.dev) is unreachable in
// kind, so the stable is brought up through the in-cluster mirror and therefore
// records the mirror base URL (not the literal default). The property under test --
// "a --ollaya-registry change never rolls a Ready stable, and recovery pulls from
// the recorded registry" -- is independent of which concrete URL was recorded; the
// asserts compare against the recorded value, never a hard-coded default.
//
// It runs through the mirror and is laya-specific (the mirror image bakes in
// laya:en). Nightly only and slow (model loads), so it shares the "registry-outage"
// label (same nightly job) and "nightly".
var _ = Describe("Registry flag change never rolls a stable", Label("registry-outage", "nightly"), Ordered, func() {
	const dm = registryFlagDM

	// mirrorURL is the --ollaya-registry value the stable is brought up with and
	// therefore the URL the stable revision records. mirrorURLAlt is the same mirror
	// Service by its fully-qualified cluster-local name: a genuinely different flag
	// string that still resolves to the same mirror, so step "change the flag" is a
	// real --ollaya-registry change (new manager generation, manager rollout) with
	// no reachability change.
	var (
		mirrorURL    = "http://" + mirrorHost
		mirrorURLAlt = "http://" + mirrorHost + ".cluster.local"

		stableS     string // the stable revision hash brought up through the mirror
		recordedReg string // status.stableRevision.registry recorded for stableS
		genBefore   string // serving Deployment .metadata.generation before any flag change
		rsBefore    string // the serving ReplicaSet name backing stableS
	)

	BeforeAll(func() {
		if testInstall == "helm" {
			Skip("helm job runs lifecycle specs only")
		}
		if os.Getenv("E2E_MIRROR") == "" {
			Skip("set E2E_MIRROR to build the mirror image and run the registry-flag spec")
		}
		if !modelIsLaya() {
			Skip("laya-specific: the registry mirror image serves laya:en")
		}
		installAndDeploy()
		deployMirror()

		By("pointing the operator at the mirror and allow-listing both forms of the mirror host")
		// Both the short Service name and its fully-qualified name are allow-listed up
		// front so the later flag change to mirrorURLAlt is not an allow-list rejection.
		setManagerArgs(
			"--ollaya-registry="+mirrorURL,
			"--allowed-registries="+mirrorHost+","+mirrorHost+".cluster.local",
			"--allow-insecure-registries",
		)

		_, _ = utils.Kubectl("create", "ns", registryFlagNS)
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
`, dm, registryFlagNS, testDevice))
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(registryFlagNS, "decisionmodel", dm, "{.status.phase}")
		}, 8*time.Minute, 5*time.Second).Should(Equal("Ready"))

		stableS = flagMustStable()
		var err error
		recordedReg, err = utils.KubectlJSONPath(registryFlagNS, "decisionmodel", dm,
			"{.status.stableRevision.registry}")
		Expect(err).NotTo(HaveOccurred())
		Expect(recordedReg).To(Equal(mirrorURL),
			"the stable brought up through the mirror must record the mirror base URL")
		genBefore = flagDeployGeneration(stableS)
		Expect(genBefore).NotTo(BeEmpty())
		rsBefore = flagServingReplicaSet(stableS)
		Expect(rsBefore).NotTo(BeEmpty(), "could not find the serving ReplicaSet for the stable")
	})

	AfterAll(func() {
		_, _ = utils.Kubectl("delete", "ns", registryFlagNS, "--ignore-not-found")
		_, _ = utils.Kubectl("delete", "ns", mirrorNS, "--ignore-not-found")
		undeploy()
	})

	AfterEach(func() { dumpDiag(registryFlagNS, dm) })

	// Step: change --ollaya-registry on a Ready stable. The flag changes to the
	// mirror's fully-qualified name (a real new manager generation + rollout) but the
	// stable must not roll: same revision hash, same serving Deployment generation,
	// same ReplicaSet, gate True, a ready endpoint, and the recorded registry is
	// still the ORIGINAL value (no re-resolve, so no re-record).
	It("does not roll the stable when --ollaya-registry changes to a different value", func() {
		By("changing --ollaya-registry to the fully-qualified mirror name (manager rolls)")
		setManagerArgs(
			"--ollaya-registry="+mirrorURLAlt,
			"--allowed-registries="+mirrorHost+","+mirrorHost+".cluster.local",
			"--allow-insecure-registries",
		)
		By("letting the manager-restart reconcile settle (the stable must stay Ready)")
		flagAwaitDMReady()
		flagAssertStableUntouched(stableS, genBefore, rsBefore, recordedReg)
	})

	// Step: a spec change creates a candidate; the NEW revision records the registry
	// the flag currently points at (the fully-qualified mirror name). This confirms a
	// real roll DOES record the current flag -- the no-roll steps are not just the
	// operator ignoring the flag everywhere.
	It("records the current --ollaya-registry on a newly promoted stable", func() {
		By("bumping the CPU request to force a new revision (resolved + prefetched via the mirror)")
		flagPatchCPU("600m")

		var newStable string
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(registryFlagNS, "decisionmodel", dm, "{.status.phase}")
		}, 12*time.Minute, 10*time.Second).Should(Equal("Ready"), "the candidate did not promote")
		newStable = flagMustStable()
		Expect(newStable).NotTo(Equal(stableS), "a CPU change must create and promote a new revision")

		gotReg, err := utils.KubectlJSONPath(registryFlagNS, "decisionmodel", dm,
			"{.status.stableRevision.registry}")
		Expect(err).NotTo(HaveOccurred())
		Expect(gotReg).To(Equal(mirrorURLAlt),
			"a newly promoted stable must record the registry the flag currently points at")

		// Re-baseline for the following steps on the new stable.
		stableS = newStable
		recordedReg = gotReg
		genBefore = flagDeployGeneration(stableS)
		rsBefore = flagServingReplicaSet(stableS)
		Expect(rsBefore).NotTo(BeEmpty())
	})

	// Step: remove --ollaya-registry entirely (keep the mirror on --allowed-registries
	// and keep --allow-insecure-registries so step "store recovery" is not refused).
	// The effective registry falls back to the unreachable default, but a Ready stable
	// never re-resolves, so it must not roll: same hash, generation, ReplicaSet, gate,
	// endpoint, and the recorded registry stays the mirror.
	It("does not roll the stable when --ollaya-registry is removed (default fallback)", func() {
		By("removing --ollaya-registry while keeping the mirror allow-listed and insecure allowed")
		setManagerArgs(
			"--allowed-registries="+mirrorHost+","+mirrorHost+".cluster.local",
			"--allow-insecure-registries",
		)
		By("letting the manager-restart reconcile settle (the stable must stay Ready)")
		flagAwaitDMReady()
		flagAssertStableUntouched(stableS, genBefore, rsBefore, recordedReg)
	})

	// Step: store recovery while --ollaya-registry is gone. Delete the stable store
	// PVC and its serving Pod; the operator must re-prefetch from the registry the
	// revision was RECORDED with (the mirror), not the current (default) flag. Assert
	// the recovery prefetch Job carries OLLAYA_REGISTRY == the recorded mirror URL and
	// the DM returns Ready on the same hash.
	It("recovers the stable store from the recorded registry after the flag is removed", func() {
		pvc := dm + "-store-" + stableS

		By("deleting the stable store PVC and its serving Pod to force a store recovery")
		_, _ = utils.Kubectl("delete", "pvc", pvc, "-n", registryFlagNS, "--wait=false")
		_, _ = utils.Kubectl("delete", "pods", "-l",
			"decisionmodel.io/name="+dm+",decisionmodel.io/revision="+stableS, "-n", registryFlagNS,
			"--grace-period=0", "--force")

		By("the recovery prefetch Job pulls from the recorded registry (OLLAYA_REGISTRY == the mirror)")
		// Select the recovery prefetch Pod by its revision label (robust across
		// label-key dots), then read OLLAYA_REGISTRY from its container env.
		Eventually(func(g Gomega) {
			env, _ := utils.Kubectl("get", "pods", "-n", registryFlagNS,
				"-l", "decisionmodel.io/name="+dm+",decisionmodel.io/prefetch-revision="+stableS,
				"-o", "jsonpath={.items[*].spec.containers[0].env[?(@.name=='OLLAYA_REGISTRY')].value}")
			g.Expect(strings.Fields(env)).To(ContainElement(recordedReg),
				"the recovery prefetch Job must pull from the recorded registry, not the current flag")
		}, 5*time.Minute, 5*time.Second).Should(Succeed())

		By("the DM returns Ready on the same stable revision once the store is re-prefetched")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(registryFlagNS, "decisionmodel", dm, "{.status.phase}")
		}, 12*time.Minute, 10*time.Second).Should(Equal("Ready"))
		Expect(flagMustStable()).To(Equal(stableS),
			"store recovery must restore the same stable revision, not create a new one")
	})
})

// ---- helpers local to the registry-flag container ----

// setManagerArgs sets the controller-manager args to the base args (everything the
// install ships, e.g. --leader-elect and the probe/metrics flags) with the three
// registry flags replaced by exactly the ones given. Unlike patchManagerArgs (which
// only appends), this can REMOVE --ollaya-registry: it first strips any existing
// --ollaya-registry / --allowed-registries / --allow-insecure-registries, then
// appends the supplied extras, and waits for the manager rollout. It does NOT wait on
// the DecisionModel (the first BeforeAll call runs before the DM exists); callers
// that need the DM to settle after the roll call flagAwaitDMReady explicitly.
func setManagerArgs(extra ...string) {
	cur, err := utils.KubectlJSONPath(operatorNamespace, "deployment", "",
		`{.items[?(@.metadata.labels.control-plane=="controller-manager")].spec.template.spec.containers[0].args[*]}`)
	Expect(err).NotTo(HaveOccurred(), "failed to read manager args")

	var kept []string
	for _, a := range strings.Fields(cur) {
		if strings.HasPrefix(a, "--ollaya-registry") ||
			strings.HasPrefix(a, "--allowed-registries") ||
			strings.HasPrefix(a, "--allow-insecure-registries") {
			continue
		}
		kept = append(kept, a)
	}
	kept = append(kept, extra...)

	var sb strings.Builder
	sb.WriteString(`{"spec":{"template":{"spec":{"containers":[{"name":"manager","args":[`)
	for i, a := range kept {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(`"` + a + `"`)
	}
	sb.WriteString(`]}]}}}}`)

	name, err := utils.KubectlJSONPath(operatorNamespace, "deployment", "",
		`{.items[?(@.metadata.labels.control-plane=="controller-manager")].metadata.name}`)
	Expect(err).NotTo(HaveOccurred())
	Expect(name).NotTo(BeEmpty(), "controller-manager Deployment not found")

	_, err = utils.Kubectl("patch", "deployment", name, "-n", operatorNamespace,
		"--type=strategic", "-p", sb.String())
	Expect(err).NotTo(HaveOccurred(), "failed to patch manager args")
	_, err = utils.Kubectl("rollout", "status", "deployment/"+name,
		"-n", operatorNamespace, "--timeout=2m")
	Expect(err).NotTo(HaveOccurred(), "manager did not roll out after the args patch")
}

// flagAwaitDMReady waits for the registry-flag DecisionModel to be Ready. It is the
// settle a caller performs AFTER setManagerArgs so a later assert reflects the flag
// change only, not a reconcile flap the manager restart triggered. Call it only once
// the DM exists (not in the first BeforeAll setManagerArgs, before the DM is created).
func flagAwaitDMReady() {
	Eventually(func() (string, error) {
		return utils.KubectlJSONPath(registryFlagNS, "decisionmodel", registryFlagDM, "{.status.phase}")
	}, 3*time.Minute, 5*time.Second).Should(Equal("Ready"))
}

// flagMustStable returns the DM's stable revision hash, failing the spec if empty.
func flagMustStable() string {
	h, err := utils.KubectlJSONPath(registryFlagNS, "decisionmodel", registryFlagDM, "{.status.stableRevision.hash}")
	Expect(err).NotTo(HaveOccurred())
	Expect(h).NotTo(BeEmpty())
	return h
}

// flagDeployGeneration returns the serving Deployment's .metadata.generation for the
// given revision (the Deployment is named <dm>-<rev>). A roll of the Pod template
// bumps this, so an unchanged value proves the stable Deployment was not re-rendered.
func flagDeployGeneration(rev string) string {
	gen, err := utils.KubectlJSONPath(registryFlagNS, "deploy", registryFlagDM+"-"+rev, "{.metadata.generation}")
	Expect(err).NotTo(HaveOccurred())
	return gen
}

// flagServingReplicaSet returns the name of the ReplicaSet backing the stable
// revision's serving Deployment. A re-render that rolls the Pod template creates a
// new ReplicaSet, so an unchanged name proves no roll happened.
func flagServingReplicaSet(rev string) string {
	out, _ := utils.Kubectl("get", "rs", "-n", registryFlagNS,
		"-l", "decisionmodel.io/revision="+rev,
		"-o", "jsonpath={.items[*].metadata.name}")
	fields := strings.Fields(out)
	if len(fields) == 0 {
		return ""
	}
	// A single serving revision has exactly one active ReplicaSet; join defensively so
	// an unexpected extra RS surfaces as a changed value rather than a silent pick.
	return strings.Join(fields, ",")
}

// flagPatchCPU sets the serving CPU request (forcing a new revision hash).
func flagPatchCPU(cpu string) {
	_, err := utils.Kubectl("patch", "decisionmodel", registryFlagDM, "-n", registryFlagNS, "--type=merge",
		"-p", fmt.Sprintf(`{"spec":{"resources":{"requests":{"cpu":%q,"memory":"1Gi"},"limits":{"memory":"4Gi"}}}}`, cpu))
	Expect(err).NotTo(HaveOccurred())
}

// flagAssertStableUntouched asserts, over a short window, that the Ready stable was
// not disturbed by a manager registry-flag change: the revision hash, the serving
// Deployment generation and the backing ReplicaSet are all unchanged, the Service
// keeps a ready endpoint, and the recorded registry is still wantReg (no re-resolve,
// so no re-record).
func flagAssertStableUntouched(stable, wantGen, wantRS, wantReg string) {
	Consistently(func(g Gomega) {
		g.Expect(flagMustStable()).To(Equal(stable),
			"a --ollaya-registry change must not change the stable revision")
		g.Expect(flagDeployGeneration(stable)).To(Equal(wantGen),
			"a --ollaya-registry change must not re-render the stable Deployment (generation bumped)")
		g.Expect(flagServingReplicaSet(stable)).To(Equal(wantRS),
			"a --ollaya-registry change must not roll the stable (new ReplicaSet)")
		eps, _ := utils.Kubectl("get", "endpoints", registryFlagDM, "-n", registryFlagNS,
			"-o", "jsonpath={.subsets[*].addresses[*].ip}")
		g.Expect(strings.Fields(eps)).NotTo(BeEmpty(),
			"the stable Service must keep a ready endpoint across a --ollaya-registry change")
		reg, _ := utils.KubectlJSONPath(registryFlagNS, "decisionmodel", registryFlagDM,
			"{.status.stableRevision.registry}")
		g.Expect(reg).To(Equal(wantReg),
			"a Ready stable does not re-resolve, so its recorded registry must not change")
	}, 30*time.Second, 5*time.Second).Should(Succeed())

	By("the stable readiness gate is still True")
	gate, _ := utils.Kubectl("get", "pods", "-l", servingPodSelector(registryFlagDM), "-n", registryFlagNS,
		"-o", "jsonpath={.items[*].status.conditions[?(@.type=='decisionmodel.io/model-ready')].status}")
	Expect(strings.Fields(gate)).To(ContainElement("True"),
		"the stable Pod's model-ready gate must stay True across a --ollaya-registry change")
}
