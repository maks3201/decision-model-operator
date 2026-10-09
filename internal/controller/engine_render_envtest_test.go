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

package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	neturl "net/url"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
	"github.com/maks3201/decision-model-operator/internal/engine/ollaya"
)

// The whole controller suite drives a fakeEngine, so a mismatch between what the
// controller passes and what the REAL engine accepts is never exercised in unit
// tests (it happened: the controller moved to 16-hex revision hashes while the
// engine's prefetch sub-path regex still accepted only 10 hex, so every prefetch
// Job rendered the refusal script and only E2E caught it). These specs build the
// reconciler with the real ollaya engine (pointed at an httptest registry, no
// network) and assert on the objects it actually renders.
var _ = Describe("rendered workloads with the real engine", func() {
	var (
		ctx       context.Context
		namespace string
		counter   int
		srv       *httptest.Server
		regHost   string
	)
	// A minimal, well-formed Ollaya manifest. Resolve hashes the raw bytes, so the
	// digest the controller records is sha256(manifestBody) and the manifest
	// ConfigMap holds exactly these bytes.
	manifestBody := []byte(`{"schemaVersion":2}`)
	wantDigest := func() string {
		sum := sha256.Sum256(manifestBody)
		return hex.EncodeToString(sum[:])
	}()

	int32Ptr := func(v int32) *int32 { return &v }
	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}
	rec := func(r *DecisionModelReconciler, name string) {
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
	}
	// markJobComplete flips a revision's prefetch Job to Complete so the next
	// reconcile moves past Caching and creates the serving Deployment. envtest has
	// no Job controller, so the status is set by hand.
	markJobComplete := func(name, rev string) {
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName(name, rev)}, job)).To(Succeed())
		now := metav1.Now()
		job.Status.StartTime, job.Status.CompletionTime = &now, &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}
	newReconciler := func() *DecisionModelReconciler {
		eng := ollaya.New(ollaya.WithRegistryURL(srv.URL))
		return &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines: map[string]engine.Engine{"ollaya": eng},
			Prober:  &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: wantDigest, Device: "cpu"}},
			// The httptest registry is an http:// host that is not ollaya.dev, so
			// the registry guard must allow it explicitly.
			AllowedRegistries:       []string{regHost},
			AllowInsecureRegistries: true,
			Recorder:                events.NewFakeRecorder(128),
		}
	}
	servingContainer := func(name, rev string) corev1.Container {
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Namespace: namespace,
			Name:      revisionName(&decisionmodelv1alpha1.DecisionModel{ObjectMeta: metav1.ObjectMeta{Name: name}}, rev),
		}, dep)).To(Succeed())
		Expect(dep.Spec.Template.Spec.Containers).NotTo(BeEmpty())
		return dep.Spec.Template.Spec.Containers[0]
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		namespace = fmt.Sprintf("engine-render-%d", counter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
		// The registry serves the same manifest for any path; Resolve only needs
		// the raw bytes to hash. No network: this is a local httptest server.
		srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write(manifestBody)
		}))
		u, perr := neturl.Parse(srv.URL)
		Expect(perr).NotTo(HaveOccurred())
		regHost = strings.ToLower(u.Host)
	})
	AfterEach(func() {
		if srv != nil {
			srv.Close()
		}
	})

	It("renders a runnable prefetch Job and serving Deployment for a 16-hex revision", func() {
		r := newReconciler()
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "real"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())

		// First reconcile: resolve against the httptest registry, record the
		// candidate and create the prefetch Job.
		rec(r, "real")
		dm := getDM("real")
		Expect(dm.Status.CandidateRevision).NotTo(BeNil(), "the candidate was recorded")
		rev := dm.Status.CandidateRevision.Hash
		Expect(dm.Status.CandidateRevision.Digest).To(Equal(wantDigest))
		Expect(rev).To(HaveLen(16), "a fresh revision is a 16-hex hash")

		By("the prefetch Job renders the pull script, not the refusal script")
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("real", rev)}, job)).To(Succeed())
		jc := job.Spec.Template.Spec.Containers[0]
		script := strings.Join(jc.Command, " ") + " " + strings.Join(jc.Args, " ")
		Expect(script).NotTo(ContainSubstring("refusing to prefetch"),
			"the engine must accept the controller's revision hash as a store sub-path")

		By("the prefetch Job's store sub-path equals the revision hash")
		var subPath string
		for _, m := range jc.VolumeMounts {
			if m.MountPath == "/models" {
				subPath = m.SubPath
			}
		}
		Expect(subPath).To(Equal(rev), "the store sub-path is the revision hash")

		By("the prefetch image is pinned (repo:tag@sha256:) for the default version")
		Expect(jc.Image).To(ContainSubstring("@sha256:"), "default image is digest-pinned")
		Expect(jc.Image).To(ContainSubstring(":" + ollaya.DefaultRuntimeVersion + "@"))

		By("the manifest is mounted from its ConfigMap (not an env var) when the ConfigMap exists")
		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "real-manifest-" + rev}, cm)).To(Succeed())
		// B-070: the engine mounts the manifest ConfigMap read-only instead of
		// carrying the bytes in an env var (argv/env is capped at 128 KiB). A volume
		// references the per-revision ConfigMap by name.
		mountsManifestCM := false
		for _, v := range job.Spec.Template.Spec.Volumes {
			if v.ConfigMap != nil && v.ConfigMap.Name == "real-manifest-"+rev {
				mountsManifestCM = true
			}
		}
		Expect(mountsManifestCM).To(BeTrue(), "the prefetch Job mounts the manifest ConfigMap when it exists")

		// Complete the Job so the controller creates the serving Deployment.
		markJobComplete("real", rev)
		rec(r, "real")

		By("the serving Pod mounts the store read-only at the same sub-path")
		sc := servingContainer("real", rev)
		var found bool
		for _, m := range sc.VolumeMounts {
			if m.MountPath == "/models" {
				found = true
				Expect(m.SubPath).To(Equal(rev), "serving sub-path matches the prefetch sub-path")
				Expect(m.ReadOnly).To(BeTrue(), "serving store mount is read-only")
			}
		}
		Expect(found).To(BeTrue(), "serving container mounts the model store")
		Expect(sc.Image).To(ContainSubstring("@sha256:"), "serving image is digest-pinned")
	})

	It("accepts a legacy 10-hex revision through the same engine render path", func() {
		// A stable revision recorded by an older operator keeps its 10-hex hash.
		// The controller renders that revision's prefetch/serving objects through
		// the same engine methods, so the engine must accept the narrower hash as a
		// store sub-path too. Render directly from a recorded 10-hex stable (what
		// the stable path passes), using the real engine.
		r := newReconciler()
		eng := r.Engines["ollaya"]
		legacyRev := wantDigest[:10] // a valid 10-hex hash
		stable := &decisionmodelv1alpha1.RevisionStatus{
			Hash: legacyRev, Engine: "ollaya", Model: "laya:en", Digest: wantDigest,
			Device: "cpu", Image: ollaya.DefaultImageCPU,
		}
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "legacy"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		}
		params := r.paramsForRevision(dm, stable, storeNameRev(dm, legacyRev))

		By("the prefetch Job for a 10-hex revision is not the refusal script")
		jobSpec := eng.PrefetchJobSpec(params)
		jc := jobSpec.Template.Spec.Containers[0]
		script := strings.Join(jc.Command, " ") + " " + strings.Join(jc.Args, " ")
		Expect(script).NotTo(ContainSubstring("refusing to prefetch"),
			"the engine must accept a 10-hex legacy revision hash as a store sub-path")
		var subPath string
		for _, m := range jc.VolumeMounts {
			if m.MountPath == "/models" {
				subPath = m.SubPath
			}
		}
		Expect(subPath).To(Equal(legacyRev))

		By("the serving Pod for a 10-hex revision mounts the store read-only at that sub-path")
		podSpec := eng.ServingPodSpec(params)
		var found bool
		for _, m := range podSpec.Containers[0].VolumeMounts {
			if m.MountPath == "/models" {
				found = true
				Expect(m.SubPath).To(Equal(legacyRev))
				Expect(m.ReadOnly).To(BeTrue())
			}
		}
		Expect(found).To(BeTrue())
	})
})
