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
	"encoding/json"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/maks3201/decision-model-operator/test/utils"
)

// operatorNamespace is where `make deploy` installs the controller-manager.
const operatorNamespace = "decision-model-operator-system"

// testNamespace is where the DecisionModel under test lives.
const testNamespace = "dmo-e2e"

// dmName is the DecisionModel (and Service) name used throughout the test.
const dmName = "support-router"

// expectedDigest is the immutable manifest digest of laya:en (bare hex),
// established in spike. status.stableRevision.digest must equal it.
const expectedDigest = "c305a9276531a47000bf93559d2c94f1ed6cbb67055c9151084682dad7655e9d"

// modelReadyGate is the Pod readiness gate condition type.
const modelReadyGate = "decisionmodel.io/model-ready"

// billingState is a support message that should be routed to the billing dept.
const billingState = "I was charged twice for my subscription this month, please refund the extra charge."

var _ = Describe("DecisionModel lifecycle", Label("lifecycle"), Ordered, func() {
	BeforeAll(func() {
		installOperator()

		By("creating the test namespace")
		_, _ = utils.Kubectl("create", "ns", testNamespace)
	})

	AfterAll(func() {
		By("deleting the DecisionModel and test namespace")
		_, _ = utils.Kubectl("delete", "decisionmodel", dmName, "-n", testNamespace, "--ignore-not-found")
		_, _ = utils.Kubectl("delete", "ns", testNamespace, "--ignore-not-found")

		uninstallOperator()
	})

	AfterEach(func() {
		if !CurrentSpecReport().Failed() {
			return
		}
		By("dumping diagnostics after failure")
		dump, _ := utils.Kubectl("get", "decisionmodel,deploy,pod,job,svc,pvc",
			"-n", testNamespace, "-o", "wide")
		_, _ = fmt.Fprintf(GinkgoWriter, "resources in %s:\n%s\n", testNamespace, dump)

		desc, _ := utils.Kubectl("describe", "decisionmodel", dmName, "-n", testNamespace)
		_, _ = fmt.Fprintf(GinkgoWriter, "DecisionModel describe:\n%s\n", desc)

		events, _ := utils.Kubectl("get", "events", "-n", testNamespace,
			"--sort-by=.lastTimestamp")
		_, _ = fmt.Fprintf(GinkgoWriter, "events in %s:\n%s\n", testNamespace, events)

		// Describe every Pod that is not Running (Pending/failed schedule etc.).
		names, _ := utils.Kubectl("get", "pods", "-n", testNamespace,
			"--field-selector=status.phase!=Running",
			"-o", "jsonpath={.items[*].metadata.name}")
		for _, p := range strings.Fields(names) {
			pd, _ := utils.Kubectl("describe", "pod", p, "-n", testNamespace)
			_, _ = fmt.Fprintf(GinkgoWriter, "describe pod %s:\n%s\n", p, pd)
		}

		// Node allocatable vs allocated — CPU pressure is the usual scale-out blocker.
		nodes, _ := utils.Kubectl("describe", "node")
		_, _ = fmt.Fprintf(GinkgoWriter, "node describe:\n%s\n", nodes)

		logs, _ := utils.Kubectl("logs", "-l", "control-plane=controller-manager",
			"-n", operatorNamespace, "--tail=100")
		_, _ = fmt.Fprintf(GinkgoWriter, "controller logs (tail):\n%s\n", logs)
	})

	It("becomes Ready with the pinned digest and a model-ready Pod", func() {
		By(fmt.Sprintf("applying a DecisionModel (%s, cpu, replicas 1)", testModel))
		applyDecisionModel(dmName, testModel, 1)

		By("waiting for status.phase=Ready (timeout 10m)")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(testNamespace, "decisionmodel", dmName, "{.status.phase}")
		}, 8*time.Minute, 5*time.Second).Should(Equal("Ready"),
			"DecisionModel never reached phase Ready")

		By("checking status.stableRevision.digest is pinned")
		digest, err := utils.KubectlJSONPath(testNamespace, "decisionmodel", dmName,
			"{.status.stableRevision.digest}")
		Expect(err).NotTo(HaveOccurred())
		if modelIsLaya() {
			Expect(digest).To(Equal(expectedDigest), "stableRevision.digest mismatch")
		} else {
			// Non-laya models have their own digest; assert it is a resolved bare-hex
			// sha256 rather than hard-coding each model's value.
			Expect(digest).To(MatchRegexp(`^[0-9a-f]{64}$`),
				"stableRevision.digest is not a bare-hex sha256: %q", digest)
		}

		By("checking status.stableRevision.device == the requested device")
		device, err := utils.KubectlJSONPath(testNamespace, "decisionmodel", dmName,
			"{.status.stableRevision.device}")
		Expect(err).NotTo(HaveOccurred())
		Expect(device).To(Equal(testDevice), "stableRevision.device mismatch")

		if testDevice == "cuda" {
			By("checking the serving Pod requests a GPU (limits nvidia.com/gpu)")
			gpu, err := utils.Kubectl("get", "pods", "-l", servingPodSelector(dmName),
				"-n", testNamespace,
				"-o", "jsonpath={.items[0].spec.containers[0].resources.limits.nvidia\\.com/gpu}")
			Expect(err).NotTo(HaveOccurred())
			Expect(gpu).To(Equal("1"), "expected serving Pod limit nvidia.com/gpu: 1")
		}

		By("checking a serving Pod has the model-ready readiness gate True")
		Eventually(func(g Gomega) {
			out, err := utils.Kubectl("get", "pods",
				"-l", servingPodSelector(dmName), "-n", testNamespace,
				"-o", fmt.Sprintf(
					"jsonpath={.items[*].status.conditions[?(@.type=='%s')].status}",
					modelReadyGate))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("True"),
				"no Pod reported %s=True (got %q)", modelReadyGate, out)
		}, 3*time.Minute, 5*time.Second).Should(Succeed())
	})

	It("shares one SELinux MCS level across the prefetch Job and serving Deployment, unique per DecisionModel", func() {
		// Regression: on SELinux-enforcing nodes whose CSI lacks SELinux
		// mount support, the runtime relabels the store PVC to the mounting Pod's MCS
		// level. If the prefetch Pod and the serving Pods carry different levels the
		// serving Pods get EACCES on the store. The operator derives one stable level
		// per DecisionModel and sets it on both Pod templates. kind on Ubuntu does not
		// enforce SELinux, so this asserts the template field is set/consistent rather
		// than exercising enforcement.
		const levelPath = "{.spec.template.spec.securityContext.seLinuxOptions.level}"
		levelRE := MatchRegexp(`^s0:c[0-9]+,c[0-9]+$`)

		By("reading the stable revision hash")
		rev, err := utils.KubectlJSONPath(testNamespace, "decisionmodel", dmName, "{.status.stableRevision.hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(rev).NotTo(BeEmpty(), "status.stableRevision.hash not set")
		sel := fmt.Sprintf("decisionmodel.io/name=%s,decisionmodel.io/revision=%s", dmName, rev)

		By("reading the serving Deployment Pod template SELinux level")
		depLevel, err := utils.Kubectl("get", "deploy", "-l", sel, "-n", testNamespace,
			"-o", "jsonpath={.items[0]"+levelPath[1:])
		Expect(err).NotTo(HaveOccurred())
		Expect(depLevel).To(levelRE, "serving Deployment Pod template SELinux level %q", depLevel)

		By("reading the prefetch Job Pod template SELinux level")
		jobLevel, err := utils.Kubectl("get", "job", "-l", sel, "-n", testNamespace,
			"-o", "jsonpath={.items[0]"+levelPath[1:])
		Expect(err).NotTo(HaveOccurred())
		Expect(jobLevel).To(levelRE, "prefetch Job Pod template SELinux level %q", jobLevel)

		By("asserting the prefetch Job and serving Deployment share the same level")
		Expect(jobLevel).To(Equal(depLevel), "prefetch and serving must share one SELinux level")

		By("creating a second DecisionModel and asserting it gets a different level")
		const dm2 = "support-router-2"
		applyDecisionModel(dm2, testModel, 1)
		defer func() {
			_, _ = utils.Kubectl("delete", "decisionmodel", dm2, "-n", testNamespace, "--ignore-not-found")
		}()
		// The level is set on the prefetch Job as soon as the controller creates it,
		// well before the model finishes pulling; wait only for the Job to appear.
		var dm2Level string
		Eventually(func(g Gomega) {
			out, err := utils.Kubectl("get", "job",
				"-l", "decisionmodel.io/name="+dm2, "-n", testNamespace,
				"-o", "jsonpath={.items[0]"+levelPath[1:])
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(levelRE, "second DecisionModel prefetch level %q", out)
			dm2Level = out
		}, 2*time.Minute, 5*time.Second).Should(Succeed())
		Expect(dm2Level).NotTo(Equal(depLevel), "distinct DecisionModels must get distinct SELinux levels")
	})

	It("answers /v1/systemone routing the billing case to the billing department", func() {
		if !modelIsLaya() {
			Skip("laya-specific: asserts the support-triage billing routing of laya:en")
		}
		stop := make(chan struct{})
		local := portForward(dmName, testNamespace, 11435, stop)
		defer close(stop)

		By("POST /v1/systemone with model + billing state")
		body := map[string]any{
			"model":      "laya:en",
			"keep_alive": -1,
			"state":      billingState,
			"questions": map[string]any{
				"department": map[string]any{
					"type": "choice",
					"criteria": map[string]string{
						"billing":   "billing, payments, charges, refunds",
						"technical": "technical bugs and errors",
						"sales":     "pricing and plans",
						"other":     "anything else",
					},
				},
			},
		}
		raw, _ := json.Marshal(body)
		var resp struct {
			Answers map[string]struct {
				Choice string `json:"choice"`
			} `json:"answers"`
		}
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("curl", "-sS", "--max-time", "180",
				"-X", "POST",
				fmt.Sprintf("http://%s/v1/systemone", local),
				"-H", "Content-Type: application/json",
				"--data-binary", string(raw)))
			g.Expect(err).NotTo(HaveOccurred(), "curl to /v1/systemone failed: %s", out)
			g.Expect(json.Unmarshal([]byte(out), &resp)).To(Succeed(), "bad JSON: %s", out)
			g.Expect(resp.Answers).To(HaveKey("department"), "no department answer: %s", out)
		}, 5*time.Minute, 10*time.Second).Should(Succeed())

		Expect(resp.Answers["department"].Choice).To(Equal("billing"),
			"expected the billing case to route to the billing department")
	})

	It("keeps the stable revision serving when a new revision fails to resolve", func() {
		By("pointing spec.model at a missing tag")
		_, err := utils.Kubectl("patch", "decisionmodel", dmName, "-n", testNamespace,
			"--type=merge", "-p", fmt.Sprintf(`{"spec":{"model":"%s:does-not-exist"}}`, modelBase()))
		Expect(err).NotTo(HaveOccurred())

		By("expecting the phase to go Failed/RolledBack")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(testNamespace, "decisionmodel", dmName, "{.status.phase}")
		}, 5*time.Minute, 5*time.Second).Should(BeElementOf("Failed", "RolledBack"),
			"phase should reflect the failed rollout")

		By("verifying the stable revision still answers (blue-green keeps stable)")
		stop := make(chan struct{})
		local := portForward(dmName, testNamespace, 11435, stop)
		defer close(stop)
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("curl", "-sS", "-o", "/dev/null",
				"-w", "%{http_code}", fmt.Sprintf("http://%s/", local)))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("200"), "stable Service stopped answering")
		}, 1*time.Minute, 5*time.Second).Should(Succeed())

		By("restoring a valid model for the scaling test")
		_, err = utils.Kubectl("patch", "decisionmodel", dmName, "-n", testNamespace,
			"--type=merge", "-p", fmt.Sprintf(`{"spec":{"model":"%s"}}`, testModel))
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(testNamespace, "decisionmodel", dmName, "{.status.phase}")
		}, 8*time.Minute, 5*time.Second).Should(Equal("Ready"))
	})

	It("scales to 2 model-ready Pods", func() {
		By("setting replicas: 2")
		_, err := utils.Kubectl("patch", "decisionmodel", dmName, "-n", testNamespace,
			"--type=merge", "-p", `{"spec":{"replicas":2}}`)
		Expect(err).NotTo(HaveOccurred())

		By("waiting for status.replicas.modelReady == 2")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(testNamespace, "decisionmodel", dmName,
				"{.status.replicas.modelReady}")
		}, 8*time.Minute, 5*time.Second).Should(Equal("2"),
			"expected 2 model-ready replicas")
	})

	It("keeps phase=Ready but flags Degraded/CacheNotShareable on an RWO store at replicas 2", func() {
		// Warning-only contract: an RWO model-store PVC with replicas>1 is
		// supported on a single node; the operator surfaces a Degraded=CacheNotShareable
		// warning without leaving the DecisionModel not-Ready.
		By("checking phase is still Ready with 2 model-ready replicas")
		phase, err := utils.KubectlJSONPath(testNamespace, "decisionmodel", dmName, "{.status.phase}")
		Expect(err).NotTo(HaveOccurred())
		Expect(phase).To(Equal("Ready"))
		ready, err := utils.KubectlJSONPath(testNamespace, "decisionmodel", dmName,
			"{.status.replicas.modelReady}")
		Expect(err).NotTo(HaveOccurred())
		Expect(ready).To(Equal("2"))

		By("checking the Degraded condition is present with reason CacheNotShareable")
		status, err := utils.KubectlJSONPath(testNamespace, "decisionmodel", dmName,
			"{.status.conditions[?(@.type=='Degraded')].status}")
		Expect(err).NotTo(HaveOccurred())
		Expect(status).To(Equal("True"), "expected Degraded=True at replicas 2 on RWO store")
		reason, err := utils.KubectlJSONPath(testNamespace, "decisionmodel", dmName,
			"{.status.conditions[?(@.type=='Degraded')].reason}")
		Expect(err).NotTo(HaveOccurred())
		Expect(reason).To(Equal("CacheNotShareable"))
	})
})

// applyDecisionModel applies a minimal DecisionModel manifest. No spec.image: the
// operator defaults the engine image from device (cpu -> :0.10.0, cuda -> :0.10.0-cuda),
// and rejects image overrides by default. Device comes from E2E_DEVICE (default
// cpu) so the same suite exercises the GPU path when run by the GPU workflow. Resource
// requests are small so >1 replica schedules on the CI runner.
func applyDecisionModel(name, model string, replicas int) {
	manifest := fmt.Sprintf(`apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata:
  name: %s
  namespace: %s
spec:
  engine: ollaya
  model: %s
  device: %s
  replicas: %d
  resources:
    requests:
      cpu: 250m
      memory: 1Gi
    limits:
      memory: 4Gi
`, name, testNamespace, model, testDevice, replicas)

	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = bytes.NewBufferString(manifest)
	out, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "failed to apply DecisionModel: %s", out)
}

// servingPodSelector returns a label selector that matches a DecisionModel's
// serving Pods while excluding its prefetch Job Pods. Prefetch Pods carry
// decisionmodel.io/name=<dm> too, so a bare name selector also matches the
// completed (Succeeded) prefetch Pod; the !decisionmodel.io/prefetch-revision
// clause drops it (serving Pods never carry that label).
func servingPodSelector(dm string) string {
	return "decisionmodel.io/name=" + dm + ",!decisionmodel.io/prefetch-revision"
}

// portForward starts `kubectl port-forward svc/<name> <freeLocal>:<port>` in the
// background and returns the local "127.0.0.1:<port>" address. The caller closes
// stop to terminate it.
//
//nolint:unparam // port kept explicit for readability
func portForward(name, namespace string, port int, stop chan struct{}) string {
	localPort := freePort()
	local := fmt.Sprintf("127.0.0.1:%d", localPort)
	cmd := exec.Command("kubectl", "port-forward",
		fmt.Sprintf("svc/%s", name), fmt.Sprintf("%d:%d", localPort, port),
		"-n", namespace)
	Expect(cmd.Start()).To(Succeed(), "failed to start port-forward")
	go func() {
		<-stop
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}()

	// Wait for the tunnel to accept connections.
	Eventually(func() error {
		c, err := net.DialTimeout("tcp", local, time.Second)
		if err != nil {
			return err
		}
		return c.Close()
	}, 30*time.Second, time.Second).Should(Succeed(), "port-forward tunnel never opened")
	return local
}

// freePort asks the kernel for an unused TCP port.
func freePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}
