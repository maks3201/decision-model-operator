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

// This suite covers the complete registry fix (recording the registry on the
// revision) and the API-key-ref switch on a frozen stable. It drives the REAL
// ollaya engine so OLLAYA_REGISTRY and the api-key ValueFrom env actually render.
var _ = Describe("recording the registry on a revision", func() {
	var (
		ctx       context.Context
		namespace string
		counter   int
		srv       *httptest.Server
		regHost   string
	)
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
	mkGatedPod := func(name, rev, pod string) {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: pod,
				Labels: map[string]string{decisionmodelv1alpha1.LabelName: name, decisionmodelv1alpha1.LabelRevision: rev},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, p)).To(Succeed())
		p.Status.PodIP = "10.0.0.171"
		p.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
	}
	newReconcilerFor := func(registryURL, allowHost string) *DecisionModelReconciler {
		eng := ollaya.New(ollaya.WithRegistryURL(registryURL))
		return &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:                 map[string]engine.Engine{"ollaya": eng},
			Prober:                  &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: wantDigest, Device: "cpu"}},
			AllowedRegistries:       []string{allowHost},
			AllowInsecureRegistries: true,
			Recorder:                events.NewFakeRecorder(128),
		}
	}
	stableDep := func(name, rev string) *appsv1.Deployment {
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-" + rev}, dep)).To(Succeed())
		return dep
	}
	// driveReady brings a single-replica DM to Ready through the real engine at srv.
	driveReady := func(r *DecisionModelReconciler, name string, mutate func(*decisionmodelv1alpha1.DecisionModel)) string {
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		}
		if mutate != nil {
			mutate(dm)
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
		rec := func() {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
		}
		rec()
		rev := getDM(name).Status.CandidateRevision.Hash
		markJobComplete(name, rev)
		rec()
		mkGatedPod(name, rev, name+"-a")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { rec(); return getDM(name).Status.Phase },
			"5s", "20ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
		return rev
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		namespace = fmt.Sprintf("registry-record-%d", counter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
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

	It("records the resolved registry on the candidate and stable revision", func() {
		r := newReconcilerFor(srv.URL, regHost)
		rev := driveReady(r, "rec", nil)
		st := getDM("rec").Status.StableRevision
		Expect(st).NotTo(BeNil())
		Expect(st.Registry).To(Equal(srv.URL), "the stable revision records the non-default registry it was built on")
		Expect(envValue(stableDep("rec", rev).Spec.Template.Spec.Containers[0].Env, "OLLAYA_REGISTRY")).
			To(Equal(srv.URL))
	})

	It("records the explicit host of the model name as the candidate registry, not the flag", func() {
		// A new candidate whose model name carries an explicit host must record that
		// host (https-defaulted / scheme kept) as its registry, NOT the --ollaya-
		// registry flag: the ollaya CLI pulls/serves an explicit-host name from the
		// name's host and ignores OLLAYA_REGISTRY. The controller passes dm.Spec.Model
		// to RecordedRegistry; the engine returns the name's host. Resolve targets the
		// name's host (http scheme -> the httptest server), so recording and resolving
		// agree.
		const bogusFlag = "https://unused-mirror.example:9999"
		explicitModel := "http://" + regHost + "/library/laya:en" // explicit host == srv (insecure http)
		r := &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			// The flag points at a DIFFERENT mirror; the recorded value must come from
			// the model name's host, not this flag.
			Engines:                 map[string]engine.Engine{"ollaya": ollaya.New(ollaya.WithRegistryURL(bogusFlag))},
			Prober:                  &fakeProber{loaded: engine.Loaded{Name: explicitModel, Digest: wantDigest, Device: "cpu"}},
			AllowedRegistries:       []string{regHost},
			AllowInsecureRegistries: true,
			Recorder:                events.NewFakeRecorder(64),
		}
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "explicit"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: explicitModel, Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "explicit"}})

		cand := getDM("explicit").Status.CandidateRevision
		Expect(cand).NotTo(BeNil(), "the candidate is admitted (resolve succeeded against the name's host)")
		Expect(cand.Registry).To(Equal("http://"+regHost),
			"the candidate records the model name's explicit host, not the --ollaya-registry flag")
		Expect(cand.Registry).NotTo(Equal(bogusFlag))
	})

	It("records the configured flag as the registry for a host-less candidate", func() {
		// A host-less name has no host of its own, so the recorded registry is the
		// engine's configured flag (the pull source for a host-less name).
		r := newReconcilerFor(srv.URL, regHost)
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "hostless"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "hostless"}})
		cand := getDM("hostless").Status.CandidateRevision
		Expect(cand).NotTo(BeNil())
		Expect(cand.Registry).To(Equal(srv.URL), "a host-less candidate records the configured flag")
	})

	It("does not roll the stable when the flag is reset to the default (recorded registry wins)", func() {
		// The known limit of the freeze-only fix: resetting --ollaya-registry to the
		// default dropped OLLAYA_REGISTRY from the fresh render and rolled the stable.
		// With the registry recorded on the revision the stable renders from it, so a
		// default-flag reconcile keeps the template byte-identical.
		r := newReconcilerFor(srv.URL, regHost)
		rev := driveReady(r, "reset", nil)
		Expect(getDM("reset").Status.StableRevision.Registry).To(Equal(srv.URL))
		depBefore := stableDep("reset", rev)
		genBefore := depBefore.Generation
		tmplBefore := depBefore.Spec.Template.DeepCopy()

		By("the manager restarts on the DEFAULT registry (no --ollaya-registry)")
		rDefault := &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:  map[string]engine.Engine{"ollaya": ollaya.New()}, // default registry
			Prober:   &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: wantDigest, Device: "cpu"}},
			Recorder: events.NewFakeRecorder(64),
		}
		_, _ = rDefault.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "reset"}})

		depAfter := stableDep("reset", rev)
		Expect(depAfter.Generation).To(Equal(genBefore), "resetting the flag to default must not roll a recorded stable")
		Expect(depAfter.Spec.Template).To(Equal(*tmplBefore), "the stable renders from the recorded registry, not the default flag")
		Expect(envValue(depAfter.Spec.Template.Spec.Containers[0].Env, "OLLAYA_REGISTRY")).To(Equal(srv.URL))
	})

	It("adopts the live registry onto a legacy stable without rolling it", func() {
		// Simulate an upgrade: a stable recorded by an older operator has an empty
		// Registry but its live serving Pod carries OLLAYA_REGISTRY. The controller
		// must adopt that value onto status (persist) and not roll the Deployment.
		r := newReconcilerFor(srv.URL, regHost)
		rev := driveReady(r, "legacy", nil)
		depBefore := stableDep("legacy", rev)
		genBefore := depBefore.Generation
		tmplBefore := depBefore.Spec.Template.DeepCopy()
		Expect(envValue(depBefore.Spec.Template.Spec.Containers[0].Env, "OLLAYA_REGISTRY")).To(Equal(srv.URL))

		By("clearing the recorded registry to look like a pre-Registry-field stable")
		Expect(updateDMStatus(ctx, namespace, "legacy", func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Status.StableRevision.Registry = ""
		})).To(Succeed())

		By("a reconcile adopts the live registry and does not roll")
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "legacy"}})

		Expect(getDM("legacy").Status.StableRevision.Registry).
			To(Equal(srv.URL), "the live registry is adopted onto the stable revision (persisted)")
		depAfter := stableDep("legacy", rev)
		Expect(depAfter.Generation).To(Equal(genBefore), "adoption must not roll the stable")
		Expect(depAfter.Spec.Template).To(Equal(*tmplBefore))
	})

	It("rolls the stable in place when spec.auth.apiKeySecretRef switches to another Secret/key", func() {
		// The ValueFrom exclusion in freezeEnvFromLive: the api-key env is a
		// secretKeyRef, so freezing must NOT pin it; switching the ref to a different
		// labelled Secret/key rolls the stable in place to the new secretKeyRef.
		r := newReconcilerFor(srv.URL, regHost)
		mkKey := func(n, key, val string) {
			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: n, Labels: map[string]string{decisionmodelv1alpha1.LabelAPIKey: "true"}},
				Data:       map[string][]byte{key: []byte(val)},
			})).To(Succeed())
		}
		mkKey("key-a", "token", "secret-a")
		mkKey("key-b", "token", "secret-b")

		rev := driveReady(r, "auth", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Auth = &decisionmodelv1alpha1.AuthSpec{APIKeySecretRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "key-a"}, Key: "token",
			}}
		})
		depBefore := stableDep("auth", rev)
		genBefore := depBefore.Generation
		Expect(secretRefName(depBefore.Spec.Template.Spec.Containers[0].Env, "OLLAYA_API_KEY")).To(Equal("key-a"))

		By("switching apiKeySecretRef to a different Secret")
		Expect(updateDM(ctx, namespace, "auth", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Auth.APIKeySecretRef.Name = "key-b"
		})).To(Succeed())
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "auth"}})

		depAfter := stableDep("auth", rev)
		Expect(depAfter.Spec.Template.Spec.Containers[0].Env).NotTo(BeNil())
		Expect(secretRefName(depAfter.Spec.Template.Spec.Containers[0].Env, "OLLAYA_API_KEY")).
			To(Equal("key-b"), "the serving template must point at the new Secret (freeze must not pin a ValueFrom)")
		Expect(depAfter.Generation).NotTo(Equal(genBefore), "an api-key ref switch rolls the stable in place")
	})

	It("adopts the engine default onto a default-registry legacy stable and never rolls under a mirror flag", func() {
		// A stable built on the default registry has no OLLAYA_REGISTRY env. Recorded
		// by an older operator it has Registry=="". When the flag later becomes a
		// mirror, the controller must adopt the explicit DEFAULT url (not the mirror)
		// so the stable keeps rendering with no env and never rolls.
		r := newReconcilerFor(srv.URL, regHost)
		rev := driveReady(r, "defadopt", nil)

		By("making the live stable look default-registry (no OLLAYA_REGISTRY env) and unrecorded")
		Expect(func() error {
			dep := &appsv1.Deployment{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "defadopt-" + rev}, dep); err != nil {
				return err
			}
			cleaned := make([]corev1.EnvVar, 0, len(dep.Spec.Template.Spec.Containers[0].Env))
			for _, e := range dep.Spec.Template.Spec.Containers[0].Env {
				if e.Name != "OLLAYA_REGISTRY" {
					cleaned = append(cleaned, e)
				}
			}
			dep.Spec.Template.Spec.Containers[0].Env = cleaned
			delete(dep.Annotations, specHashAnnotation)
			return k8sClient.Update(ctx, dep)
		}()).To(Succeed())
		Expect(updateDMStatus(ctx, namespace, "defadopt", func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Status.StableRevision.Registry = ""
		})).To(Succeed())

		By("a first mirror-flag reconcile adopts the default and settles the template")
		rMirror := newReconcilerFor("http://mirror.example.com:8080", "mirror.example.com:8080")
		rMirror.AllowedRegistries = []string{regHost, "mirror.example.com:8080"}
		rec := func() {
			_, _ = rMirror.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "defadopt"}})
		}
		rec()
		Expect(getDM("defadopt").Status.StableRevision.Registry).
			To(Equal("https://ollaya.dev"), "a default-registry stable adopts the explicit default URL, not the mirror flag")
		Expect(envValue(stableDep("defadopt", rev).Spec.Template.Spec.Containers[0].Env, "OLLAYA_REGISTRY")).
			To(Equal(""), "a default-registry stable must not gain OLLAYA_REGISTRY from a mirror flag")

		By("a subsequent reconcile is a no-op: no roll (generation stable)")
		genSettled := stableDep("defadopt", rev).Generation
		rec()
		Expect(stableDep("defadopt", rev).Generation).To(Equal(genSettled), "the default stable must not roll under a mirror flag")
		Expect(envValue(stableDep("defadopt", rev).Spec.Template.Spec.Containers[0].Env, "OLLAYA_REGISTRY")).To(Equal(""))
	})

	It("renders a queued/candidate from its recorded registry when the flag changes mid-rollout", func() {
		// A candidate admitted under registry A must keep rendering against A even if
		// the operator flag flips to B before the candidate starts, so its serving
		// Pods / prefetch Job match the store prefetched under A.
		r := newReconcilerFor(srv.URL, regHost)
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "midroll"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
		// First reconcile records the candidate (Registry = srv.URL = A) and the Job.
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "midroll"}})
		cand := getDM("midroll").Status.CandidateRevision
		Expect(cand).NotTo(BeNil())
		Expect(cand.Registry).To(Equal(srv.URL))
		rev := cand.Hash

		By("the prefetch Job carries registry A")
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("midroll", rev)}, job)).To(Succeed())
		Expect(envValue(job.Spec.Template.Spec.Containers[0].Env, "OLLAYA_REGISTRY")).To(Equal(srv.URL))

		By("the flag flips to B, the Job completes, the serving Deployment is created")
		markJobComplete("midroll", rev)
		srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(manifestBody) }))
		defer srv2.Close()
		u2, _ := neturl.Parse(srv2.URL)
		host2 := strings.ToLower(u2.Host)
		rB := newReconcilerFor(srv2.URL, host2)
		rB.AllowedRegistries = []string{regHost, host2}
		_, _ = rB.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "midroll"}})

		By("the candidate serving Pod template still carries registry A, not B")
		dep := stableDep("midroll", rev) // candidate Deployment is <dm>-<rev>
		Expect(envValue(dep.Spec.Template.Spec.Containers[0].Env, "OLLAYA_REGISTRY")).
			To(Equal(srv.URL), "a mid-rollout flag change must not re-render the candidate against a different registry")
	})

	It("blocks store recovery when the recorded registry is no longer allowed", func() {
		// Drive the recovery path directly (as the store-recovery suite does): a stable
		// whose recorded Registry host is no longer in --allowed-registries must not get
		// a recovery prefetch Job, and must go Degraded with RegistryNotAllowed.
		rTight := newReconcilerFor(srv.URL, regHost)
		rTight.AllowedRegistries = []string{"other.example.com"} // srv host no longer allowed
		rTight.AllowInsecureRegistries = true
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "recguard"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
		stable := &decisionmodelv1alpha1.RevisionStatus{
			Hash: "r1", Engine: "ollaya", Model: "laya:en", Digest: wantDigest, Device: "cpu",
			Image: fakeImage, Registry: srv.URL, // recorded a now-disallowed registry
		}
		dm.Status.StableRevision = stable
		claim := storeNameRev(dm, stable.Hash)

		By("recovery recreates the annotated PVC, then refuses the prefetch for a disallowed registry")
		// First pass: PVC missing -> recreate (annotated), handled.
		handled, _, _, err := rTight.recoverStableStore(ctx, dm, rTight.Engines["ollaya"], stable, claim, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(handled).To(BeTrue())
		// Next pass: PVC present + recovering, no Job -> guard blocks before creating one.
		handled, _, _, err = rTight.recoverStableStore(ctx, dm, rTight.Engines["ollaya"], stable, claim, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(handled).To(BeTrue())

		recJob := &batchv1.Job{}
		gerr := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("recguard", "r1")}, recJob)
		Expect(gerr).To(HaveOccurred(), "no recovery prefetch Job must be created for a disallowed registry")
		deg := meta_Find(dm, decisionmodelv1alpha1.ConditionDegraded)
		Expect(deg).NotTo(BeNil())
		Expect(deg.Status).To(Equal(metav1.ConditionTrue))
		Expect(deg.Reason).To(Equal(reasonRegistryNotAllowed))
	})
})

// secretRefName returns the Secret name backing the named env var's
// ValueFrom.SecretKeyRef, or "" if absent/not a secretKeyRef.
func secretRefName(env []corev1.EnvVar, name string) string {
	for _, e := range env {
		if e.Name == name && e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil {
			return e.ValueFrom.SecretKeyRef.Name
		}
	}
	return ""
}
