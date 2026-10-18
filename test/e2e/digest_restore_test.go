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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/maks3201/decision-model-operator/test/utils"
)

// digestRestoreMirrorNS holds this spec's own registry mirror; digestRestoreNS holds
// the DecisionModel under test. The spec runs its own mirror (not the chaos/mirror
// one) so it can flip MIRROR_TAG_MOVED without racing another container's teardown.
const digestRestoreMirrorNS = "dmo-e2e-digest-restore-mirror"
const digestRestoreNS = "dmo-e2e-digest-restore"

// digestRestoreMirrorHost is the in-cluster DNS name of this spec's mirror Service.
var digestRestoreMirrorHost = fmt.Sprintf("mirror.%s.svc", digestRestoreMirrorNS)

// This spike-011 scenario proves the value of pulling by digest on Ollaya 0.13.0:
// after a model's upstream tag moves to a different revision, a lost store is
// restored to the exact PINNED revision the operator recorded, not whatever the tag
// now points at. The recovery prefetch pulls <model>@sha256:<recorded-digest>, so a
// moved tag can neither substitute a new revision (no silent swap) nor fail the
// recovery (no UpstreamTagMoved). Nightly-only: it loads laya:en and drives a store
// recovery. Runs through this spec's in-cluster mirror so the tag can be moved while
// the digest route keeps serving the original bytes.
var _ = Describe("Digest restore: a moved tag does not change a recovered revision",
	Label("nightly", "chaos"), Ordered, func() {
		const dm = "digest-restore-router"

		BeforeAll(func() {
			if testInstall == "helm" {
				Skip("helm job runs lifecycle specs only")
			}
			if os.Getenv("E2E_MIRROR") == "" {
				Skip("set E2E_MIRROR to build the mirror image and run this spec")
			}
			if !modelIsLaya() {
				Skip("laya-specific: the registry mirror image serves laya:en")
			}

			installAndDeploy()
			deployDigestRestoreMirror()

			By("pointing the operator at this spec's mirror")
			patchManagerArgs(
				"--ollaya-registry=http://"+digestRestoreMirrorHost,
				"--allowed-registries="+digestRestoreMirrorHost,
				"--allow-insecure-registries",
			)

			_, _ = utils.Kubectl("create", "ns", digestRestoreNS)
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
`, dm, digestRestoreNS, testDevice))
			Eventually(func() (string, error) {
				return utils.KubectlJSONPath(digestRestoreNS, "decisionmodel", dm, "{.status.phase}")
			}, 8*time.Minute, 5*time.Second).Should(Equal("Ready"),
				"the stable did not reach Ready through the mirror")
		})

		AfterAll(func() {
			_, _ = utils.Kubectl("delete", "ns", digestRestoreNS, "--ignore-not-found")
			_, _ = utils.Kubectl("delete", "ns", digestRestoreMirrorNS, "--ignore-not-found")
			scaleManager(1)
			undeploy()
			cluster := envOr("KIND_CLUSTER", "kind")
			_, _ = utils.Run(exec.Command("docker", "exec", cluster+"-control-plane",
				"crictl", "rmi", "docker.io/library/"+mirrorImage))
		})

		AfterEach(func() { dumpDiag(digestRestoreNS, dm) })

		It("restores the pinned revision after the tag moved, not the tag's new target", func() {
			By("recording the pinned digest of the stable revision")
			pinnedDigest, err := utils.KubectlJSONPath(digestRestoreNS, "decisionmodel", dm,
				"{.status.stableRevision.digest}")
			Expect(err).NotTo(HaveOccurred())
			Expect(pinnedDigest).To(MatchRegexp(`^[0-9a-f]{64}$`),
				"stableRevision.digest is not a bare-hex sha256: %q", pinnedDigest)
			stableHash, err := utils.KubectlJSONPath(digestRestoreNS, "decisionmodel", dm,
				"{.status.stableRevision.hash}")
			Expect(err).NotTo(HaveOccurred())
			Expect(stableHash).NotTo(BeEmpty())

			// Deleting the store inside the post-promotion stabilization window would
			// make the controller roll back to the prior revision instead of recovering
			// the store. The first revision has no previous one, so previousRevision is
			// empty from the start; poll to be explicit and robust if an earlier step
			// ever promotes.
			By("confirming no stabilization window is open (previousRevision empty)")
			Eventually(func() (string, error) {
				return utils.KubectlJSONPath(digestRestoreNS, "decisionmodel", dm, "{.status.previousRevision}")
			}, stabilizationWait, 5*time.Second).Should(BeEmpty())

			By("moving the mirror's laya:en tag to a different manifest (upstream repackage)")
			// A tag pull from now on returns a byte-different revision; the digest route
			// still serves the pinned bytes. The flag is read at startup, so a rollout
			// is needed for it to take effect (same mechanism as MIRROR_FAULT).
			setDigestRestoreTagMoved(true)

			By("deleting the stable store PVC and its serving Pod to force a store recovery")
			pvc := dm + "-store-" + stableHash
			_, _ = utils.Kubectl("delete", "pvc", pvc, "-n", digestRestoreNS, "--wait=false")
			_, _ = utils.Kubectl("delete", "pods",
				"-l", "decisionmodel.io/name="+dm+",decisionmodel.io/revision="+stableHash,
				"-n", digestRestoreNS, "--grace-period=0", "--force")

			By("the DM returns Ready on the SAME stable revision (store recovery succeeded)")
			Eventually(func() (string, error) {
				return utils.KubectlJSONPath(digestRestoreNS, "decisionmodel", dm, "{.status.phase}")
			}, 12*time.Minute, 10*time.Second).Should(Equal("Ready"),
				"store recovery did not return the DM to Ready")
			gotHash, err := utils.KubectlJSONPath(digestRestoreNS, "decisionmodel", dm,
				"{.status.stableRevision.hash}")
			Expect(err).NotTo(HaveOccurred())
			Expect(gotHash).To(Equal(stableHash),
				"store recovery must restore the same stable revision, not a new one")

			By("the recovered stable still records the PINNED digest (unchanged by the moved tag)")
			gotDigest, err := utils.KubectlJSONPath(digestRestoreNS, "decisionmodel", dm,
				"{.status.stableRevision.digest}")
			Expect(err).NotTo(HaveOccurred())
			Expect(gotDigest).To(Equal(pinnedDigest),
				"the recovered revision's digest must equal the originally pinned digest")

			By("a recovered serving Pod has the model-ready gate True (api/ps shows the pinned digest+device)")
			// The readiness gate is True only when the runtime's /api/ps reports the
			// expected digest on the expected device; a True gate is the operator's own
			// proof that /api/ps serves the pinned revision.
			Eventually(func(g Gomega) {
				out, err := utils.Kubectl("get", "pods",
					"-l", servingPodSelector(dm), "-n", digestRestoreNS,
					"-o", fmt.Sprintf(
						"jsonpath={.items[*].status.conditions[?(@.type=='%s')].status}",
						modelReadyGate))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(ContainSubstring("True"),
					"no recovered Pod reported %s=True (got %q)", modelReadyGate, out)
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("querying /api/ps on the recovered Pod directly: it reports the ORIGINAL digest")
			Expect(apiPSDigest(dm)).To(Equal(pinnedDigest),
				"/api/ps on the recovered Pod must report the originally pinned digest")

			By("no revision was marked failed and no moved-tag / prefetch failure was recorded")
			failed, err := utils.KubectlJSONPath(digestRestoreNS, "decisionmodel", dm, "{.status.failedRevision}")
			Expect(err).NotTo(HaveOccurred())
			Expect(failed).To(BeEmpty(), "store recovery by digest must not fail the revision")
			Expect(digestRestoreEventCount("UpstreamTagMoved")).To(Equal(0),
				"a digest pull must never report UpstreamTagMoved")
			Expect(digestRestoreEventCount("StorePrefetchFailed")).To(Equal(0),
				"the recovery prefetch by digest must not fail")
		})
	})

// deployDigestRestoreMirror brings up this spec's own registry mirror (the same
// baked image as the chaos/mirror spec) in digestRestoreMirrorNS and waits for it to
// be Available. Kept separate from deployMirror so flipping MIRROR_TAG_MOVED here
// never races the chaos container's mirror lifecycle.
func deployDigestRestoreMirror() {
	ensureMirrorImageOnNode()
	_, _ = utils.Kubectl("create", "ns", digestRestoreMirrorNS)
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
          httpGet: {path: /healthz, port: 8080}
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
`, digestRestoreMirrorNS, mirrorImage, digestRestoreMirrorNS))
	_, err := utils.Kubectl("wait", "deployment/mirror", "-n", digestRestoreMirrorNS,
		"--for=condition=Available", "--timeout=2m")
	Expect(err).NotTo(HaveOccurred(), "digest-restore mirror did not become Available")
}

// setDigestRestoreTagMoved flips the mirror's MIRROR_TAG_MOVED mode and waits for the
// rollout, so every subsequent tag pull returns a byte-different manifest while the
// digest route keeps serving the original bytes.
func setDigestRestoreTagMoved(on bool) {
	val := "MIRROR_TAG_MOVED-"
	if on {
		val = "MIRROR_TAG_MOVED=1"
	}
	_, err := utils.Kubectl("set", "env", "deployment/mirror", "-n", digestRestoreMirrorNS, val)
	Expect(err).NotTo(HaveOccurred())
	_, err = utils.Kubectl("rollout", "status", "deployment/mirror",
		"-n", digestRestoreMirrorNS, "--timeout=2m")
	Expect(err).NotTo(HaveOccurred(), "mirror did not roll out with MIRROR_TAG_MOVED")
}

// apiPSDigest port-forwards to the DecisionModel's serving Service and returns the
// bare-hex digest the runtime reports for the loaded model at /api/ps. It is the
// direct, runtime-side confirmation that the recovered Pod serves the pinned
// revision (the readiness gate asserts the same thing operator-side).
func apiPSDigest(dm string) string {
	stop := make(chan struct{})
	defer close(stop)
	local := portForward(dm, digestRestoreNS, 11435, stop)

	var digest string
	Eventually(func(g Gomega) {
		out, err := utils.Run(exec.Command("curl", "-sS", "--max-time", "15",
			fmt.Sprintf("http://%s/api/ps", local)))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(out).To(ContainSubstring(`"digest"`), "unexpected /api/ps body: %s", out)
		digest = firstJSONStringField(out, "digest")
		g.Expect(digest).To(MatchRegexp(`^[0-9a-f]{64}$`), "no bare-hex digest in /api/ps: %s", out)
	}, 1*time.Minute, 3*time.Second).Should(Succeed())
	return digest
}

// firstJSONStringField extracts the value of the first `"<field>":"<value>"` pair
// from a JSON body without a full unmarshal (the /api/ps shape is fixed and small,
// and the only string digest in it is the model digest). Returns "" if absent.
func firstJSONStringField(body, field string) string {
	key := `"` + field + `":"`
	i := strings.Index(body, key)
	if i < 0 {
		return ""
	}
	rest := body[i+len(key):]
	j := strings.IndexByte(rest, '"')
	if j < 0 {
		return ""
	}
	return rest[:j]
}

// digestRestoreEventCount counts Events on the DecisionModel in this spec's
// namespace by reason.
func digestRestoreEventCount(reason string) int {
	out, _ := utils.Kubectl("get", "events", "-n", digestRestoreNS,
		"--field-selector", "involvedObject.name="+"digest-restore-router"+",reason="+reason,
		"-o", "jsonpath={.items[*].reason}")
	return len(strings.Fields(out))
}
