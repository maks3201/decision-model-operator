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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/maks3201/decision-model-operator/test/utils"
)

// mirrorNS holds the in-cluster registry mirror; dmMirrorNS holds the DecisionModel
// under test. Separate namespaces keep the mirror's lifecycle independent of the DM.
const mirrorNS = "dmo-e2e-mirror"
const dmMirrorNS = "dmo-e2e-mirror-dm"

// mirrorImage is the registry-mirror image built and loaded by hack/mirror-build.sh.
// It bakes in the laya:en store and a tiny static Ollaya/Docker-v2 registry server,
// so the operator's resolve and the prefetch Job's pull run offline inside kind.
var mirrorImage = envOr("MIRROR_IMAGE", "dmo-e2e-mirror:laya-en")

// mirrorHost is the in-cluster DNS name of the mirror Service. The DecisionModel's
// model name is host-less (laya:en), so the operator targets this base URL via
// --ollaya-registry, and it must be on the --allowed-registries list.
var mirrorHost = fmt.Sprintf("mirror.%s.svc", mirrorNS)

var _ = Describe("Registry mirror", Ordered, func() {
	const dm = "mirror-router"

	BeforeAll(func() {
		if testInstall == "helm" {
			Skip("helm job runs lifecycle specs only")
		}
		// The mirror image is built + loaded in BeforeSuite only when E2E_MIRROR is
		// set (e2e_suite_test.go). The mirror Pod uses imagePullPolicy: Never, so
		// without that image the spec cannot run — skip unless E2E_MIRROR is set so a
		// plain `make test-e2e` stays green. CI sets E2E_MIRROR in
		// test-e2e.yml for the kustomize job.
		if os.Getenv("E2E_MIRROR") == "" {
			Skip("set E2E_MIRROR to build the mirror image and run this spec")
		}

		// hack/mirror-build.sh fetched the manifest+blobs over HTTP (no ollaya) and
		// loaded the mirror image into the kind node in BeforeSuite.
		installAndDeploy()

		By("deploying the mirror Deployment + Service")
		_, _ = utils.Kubectl("create", "ns", mirrorNS)
		applyYAML(fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata: {name: mirror, namespace: %s}
spec:
  replicas: 1
  selector: {matchLabels: {app: mirror}}
  template:
    metadata: {labels: {app: mirror}}
    spec:
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        seccompProfile: {type: RuntimeDefault}
      containers:
      - name: mirror
        image: %s
        imagePullPolicy: Never
        ports: [{containerPort: 8080}]
        readinessProbe:
          httpGet: {path: /v2/, port: 8080}
          periodSeconds: 2
        securityContext:
          allowPrivilegeEscalation: false
          readOnlyRootFilesystem: true
          runAsNonRoot: true
          capabilities: {drop: [ALL]}
---
apiVersion: v1
kind: Service
metadata: {name: mirror, namespace: %s}
spec:
  selector: {app: mirror}
  ports: [{port: 80, targetPort: 8080}]
`, mirrorNS, mirrorImage, mirrorNS))

		By("waiting for the mirror to be Available")
		_, err := utils.Kubectl("wait", "deployment/mirror", "-n", mirrorNS,
			"--for=condition=Available", "--timeout=2m")
		Expect(err).NotTo(HaveOccurred(), "mirror Deployment did not become Available")

		By("pointing the operator at the mirror (--ollaya-registry, allow-list, insecure)")
		// http:// base => non-TLS pull; the mirror host must be allow-listed (SSRF guard).
		// stdlib flag is last-wins, so appending --allowed-registries overrides the default.
		patchManagerArgs(
			"--ollaya-registry=http://"+mirrorHost,
			"--allowed-registries="+mirrorHost,
			"--allow-insecure-registries",
		)
	})

	AfterAll(func() {
		_, _ = utils.Kubectl("delete", "ns", dmMirrorNS, "--ignore-not-found")
		_, _ = utils.Kubectl("delete", "ns", mirrorNS, "--ignore-not-found")
		undeploy()
		// Reclaim the mirror image from the kind node so it does not linger on a
		// disk-constrained runner (the mirror image + a model copy pushed
		// the GitHub runner out of space). Best-effort: never fail the spec on it.
		cluster := envOr("KIND_CLUSTER", "kind")
		_, _ = utils.Run(exec.Command("docker", "exec", cluster+"-control-plane",
			"crictl", "rmi", "docker.io/library/"+mirrorImage))
	})

	AfterEach(func() { dumpDiag(dmMirrorNS, dm) })

	It("resolves and prefetches laya:en from the in-cluster mirror; digest matches", func() {
		By("recording the digest the mirror serves for laya:en")
		// The operator records sha256(manifest bytes) of whatever the registry returns;
		// our mirror strips the manifest's blob "urls" (so the pull stays offline), which
		// changes the bytes. Compute the expected digest from the mirror itself so the
		// assertion is self-checking and never hard-codes a value.
		wantDigest := mirrorManifestDigest("library/laya", "en")
		Expect(wantDigest).To(HaveLen(64), "mirror manifest digest should be 64 hex chars")
		_, _ = fmt.Fprintf(GinkgoWriter, "mirror laya:en digest=%s\n", wantDigest)

		By("creating a DecisionModel with a host-less model name (laya:en)")
		_, _ = utils.Kubectl("create", "ns", dmMirrorNS)
		applyYAML(fmt.Sprintf(`apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata: {name: %s, namespace: %s}
spec:
  engine: ollaya
  model: laya:en
  device: %s
  replicas: 1
  resources: {requests: {cpu: 250m, memory: 1Gi}, limits: {memory: 4Gi}}
`, dm, dmMirrorNS, testDevice))

		By("the DM reaches Ready (resolve + prefetch Job both went through the mirror)")
		Eventually(func() (string, error) {
			return utils.KubectlJSONPath(dmMirrorNS, "decisionmodel", dm, "{.status.phase}")
		}, 8*time.Minute, 5*time.Second).Should(Equal("Ready"),
			"DM did not reach Ready pulling from the mirror")

		By("status records the digest the mirror served")
		gotDigest, err := utils.KubectlJSONPath(dmMirrorNS, "decisionmodel", dm,
			"{.status.stableRevision.digest}")
		Expect(err).NotTo(HaveOccurred())
		Expect(gotDigest).To(Equal(wantDigest),
			"recorded digest must equal sha256 of the mirror's manifest bytes")

		By("the mirror actually served the manifest and the model blobs")
		// The Job pulled from the mirror only (no upstream): the mirror log shows one
		// manifest hit and >= 2 blob hits (the two large onnx/weights layers at minimum).
		logs, err := utils.Kubectl("logs", "deployment/mirror", "-n", mirrorNS, "--tail=200")
		Expect(err).NotTo(HaveOccurred())
		Expect(logs).To(ContainSubstring("manifest library/laya/en"),
			"mirror never served the laya:en manifest")
		Expect(strings.Count(logs, "200 blob")).To(BeNumerically(">=", 2),
			"mirror served fewer blobs than expected (pull may have bypassed the mirror):\n%s", logs)
	})
})

// patchManagerArgs appends extra flags to the controller-manager container's args
// and waits for the new generation to roll out. Works for both the kustomize and
// helm install paths (the container is named "manager" in both).
func patchManagerArgs(extra ...string) {
	cur, err := utils.KubectlJSONPath(operatorNamespace, "deployment", "",
		`{.items[?(@.metadata.labels.control-plane=="controller-manager")].spec.template.spec.containers[0].args[*]}`)
	Expect(err).NotTo(HaveOccurred(), "failed to read manager args")
	args := strings.Fields(cur)
	args = append(args, extra...)

	// Build a JSON array for a strategic-merge patch of the manager container's args.
	var sb strings.Builder
	sb.WriteString(`{"spec":{"template":{"spec":{"containers":[{"name":"manager","args":[`)
	for i, a := range args {
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

// mirrorManifestDigest fetches the manifest the mirror serves for repo:tag and
// returns sha256(bytes) as bare hex — exactly what the operator's resolver records.
func mirrorManifestDigest(repo, tag string) string {
	stop := make(chan struct{})
	local := portForward("mirror", mirrorNS, 80, stop)
	defer close(stop)

	var body string
	Eventually(func(g Gomega) {
		out, err := utils.Run(exec.Command("curl", "-sS", "--max-time", "30",
			fmt.Sprintf("http://%s/v2/%s/manifests/%s", local, repo, tag)))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(out).NotTo(BeEmpty(), "mirror returned an empty manifest")
		g.Expect(out).To(ContainSubstring(`"schemaVersion"`), "not a manifest: %s", out)
		body = out
	}, 1*time.Minute, 3*time.Second).Should(Succeed())

	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}
