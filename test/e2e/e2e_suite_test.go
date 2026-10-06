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
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/maks3201/decision-model-operator/test/utils"
)

// projectImage is the operator image built from the current source, loaded into
// the kind cluster and deployed by the suite.
const projectImage = "example.com/decision-model-operator:v0.0.1-e2e"

// testDevice selects the DecisionModel device under test. Default "cpu" keeps the
// CPU CI identical; the GPU workflow sets E2E_DEVICE=cuda.
var testDevice = envOr("E2E_DEVICE", "cpu")

// ollayaImage is the runtime image the DecisionModel Pods/Jobs use. It is loaded
// into the kind cluster so nodes never pull from the network. Defaults to the CPU
// tag; the GPU workflow overrides it via OLLAYA_IMAGE (…:0.10.0-cuda).
var ollayaImage = envOr("OLLAYA_IMAGE", "ghcr.io/ollaya-dev/ollaya:0.10.0")

// envOr returns the env var value or a default when unset/empty.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// TestE2E runs the end-to-end (e2e) test suite for the project. It exercises a
// full DecisionModel lifecycle on a kind cluster with the Ollaya runtime.
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting decision-model-operator e2e test suite (device=%s image=%s)\n",
		testDevice, ollayaImage)
	RunSpecs(t, "e2e suite")
}

var _ = BeforeSuite(func() {
	By("recording runner capacity (nproc, free -m)")
	if out, err := exec.Command("nproc").CombinedOutput(); err == nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "runner nproc: %s", out)
	}
	if out, err := exec.Command("free", "-m").CombinedOutput(); err == nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "runner free -m:\n%s", out)
	}

	By("snapshotting config/manager/kustomization.yaml (make deploy mutates it)")
	restoreKustomization()

	By("ensuring a kind cluster and the Ollaya runtime image (hack/kind-up.sh)")
	cmd := exec.Command("./hack/kind-up.sh")
	cmd.Env = append(os.Environ(), "OLLAYA_IMAGE="+ollayaImage)
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to bring up the kind cluster / load the Ollaya image")

	By("building the manager (operator) image")
	cmd = exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", projectImage))
	_, err = utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to build the manager image")

	By("loading the manager image into the kind cluster")
	err = utils.LoadImageToKindClusterWithName(projectImage)
	Expect(err).NotTo(HaveOccurred(), "Failed to load the manager image into kind")

	// Build + load the registry-mirror image for the "Registry mirror" spec, which
	// runs only on the kustomize job with E2E_MIRROR set (set in test-e2e.yml). The
	// helm job skips that spec (lifecycle only) and does not need the image;
	// gating also avoids fetching ~850 MiB when the spec is not being run.
	if testInstall != "helm" && os.Getenv("E2E_MIRROR") != "" {
		By("building and loading the in-cluster registry mirror image (hack/mirror-build.sh)")
		cmd = exec.Command("./hack/mirror-build.sh")
		cmd.Env = append(os.Environ(), "MIRROR_IMAGE="+mirrorImage)
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to build/load the mirror image")
	}
})

// testInstall selects how the operator is installed: "kustomize" (default, via
// make install+deploy) or "helm" (via the chart). The GPU/lifecycle helm CI job
// sets E2E_INSTALL=helm.
var testInstall = envOr("E2E_INSTALL", "kustomize")

// installOperator installs CRDs + controller-manager into operatorNamespace using
// the method selected by E2E_INSTALL, then waits for it to be Available.
func installOperator() {
	switch testInstall {
	case "helm":
		By("installing the operator via the Helm chart")
		repo, tag := splitImage(projectImage)
		_, err := utils.Run(exec.Command("helm", "install", helmRelease,
			"charts/decision-model-operator",
			"--namespace", operatorNamespace, "--create-namespace",
			"--set", "image.repository="+repo,
			"--set", "image.tag="+tag,
			"--set", "image.pullPolicy=IfNotPresent",
			"--wait", "--timeout", "3m"))
		Expect(err).NotTo(HaveOccurred(), "helm install failed")
	default:
		By("installing CRDs (kustomize)")
		_, err := utils.Run(exec.Command("make", "install"))
		Expect(err).NotTo(HaveOccurred(), "make install failed")
		By("deploying the controller-manager (kustomize)")
		_, err = utils.Run(exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", projectImage)))
		Expect(err).NotTo(HaveOccurred(), "make deploy failed")
	}

	By("waiting for the controller-manager to be Available")
	_, err := utils.Kubectl("wait", "deployment.apps",
		"-l", "control-plane=controller-manager",
		"--for=condition=Available", "-n", operatorNamespace, "--timeout=3m")
	Expect(err).NotTo(HaveOccurred(), "controller-manager did not become Available")
}

// uninstallOperator tears the operator down with the matching method.
func uninstallOperator() {
	if testInstall == "helm" {
		By("uninstalling the Helm release")
		_, _ = utils.Run(exec.Command("helm", "uninstall", helmRelease,
			"--namespace", operatorNamespace, "--wait", "--timeout", "2m"))
		return
	}
	By("undeploying the controller-manager and CRDs (kustomize)")
	_, _ = utils.Run(exec.Command("make", "undeploy"))
	_, _ = utils.Run(exec.Command("make", "uninstall"))
}

// helmRelease is the release name for the helm install path.
const helmRelease = "dmo"

// splitImage splits "repo:tag" at the last colon.
func splitImage(image string) (repo, tag string) {
	i := strings.LastIndex(image, ":")
	if i < 0 {
		return image, ""
	}
	return image[:i], image[i+1:]
}

// restoreKustomization saves config/manager/kustomization.yaml and registers a
// DeferCleanup to restore it, so `make deploy` (which runs `kustomize edit set image`)
// does not leave the working tree dirty after the suite.
func restoreKustomization() {
	dir, err := utils.GetProjectDir()
	Expect(err).NotTo(HaveOccurred())
	path := filepath.Join(dir, "config", "manager", "kustomization.yaml")
	orig, err := os.ReadFile(path)
	Expect(err).NotTo(HaveOccurred(), "failed to read %s", path)
	DeferCleanup(func() {
		Expect(os.WriteFile(path, orig, 0o644)).To(Succeed(),
			"failed to restore %s", path)
	})
}
