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
	"fmt"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
	"github.com/maks3201/decision-model-operator/internal/engine/ollaya"
)

// --- fake engine ---------------------------------------------------------

const (
	fakeImage       = "ghcr.io/ollaya-dev/ollaya:test"
	fakeImageCUDA   = "ghcr.io/ollaya-dev/ollaya:test-cuda"
	fakeServingPort = int32(11435)
	// defaultDigest is the digest the fake engine resolves to by default.
	defaultDigest = "abc1230000000000000000000000000000000000000000000000000000000000"
)

// fakeEngine is a test double implementing engine.Engine without any network.
type fakeEngine struct {
	mu           sync.Mutex
	digest       string
	resolveErr   error
	resolveCalls int
}

// newFakeEngine builds a fake engine seeded with the default test digest;
// tests that need a second digest set eng.digest directly.
func newFakeEngine() *fakeEngine { return &fakeEngine{digest: defaultDigest} }

func (f *fakeEngine) Name() string { return "ollaya" }

func (f *fakeEngine) Resolve(_ context.Context, name string) (engine.ModelRef, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resolveCalls++
	if f.resolveErr != nil {
		return engine.ModelRef{}, f.resolveErr
	}
	return engine.ModelRef{Name: name, Digest: f.digest}, nil
}

func (f *fakeEngine) resolveCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.resolveCalls
}

// CanonicalName lowercases a valid name and rejects obviously invalid ones
// (empty, whitespace, or a CLI-flag-looking value), mirroring the real engine's
// validation closely enough for controller tests.
func (f *fakeEngine) CanonicalName(name string) (string, error) {
	n := strings.TrimSpace(name)
	if n == "" || strings.ContainsAny(n, " \t") || strings.HasPrefix(n, "-") {
		return "", fmt.Errorf("invalid model name %q", name)
	}
	return strings.ToLower(n), nil
}

// RegistryHost reports the registry host a model name targets, delegating to the
// real ollaya parser so the fake engine matches production host resolution and
// satisfies engine.RegistryHoster (engines without it are denied).
func (f *fakeEngine) RegistryHost(name string) (string, bool, error) {
	return ollaya.New().RegistryHost(name)
}

// fakeImageFor mirrors the real engine's imageFor: an explicit p.Image override
// wins; otherwise, when a runtime version is set, a version-specific tag
// (so a pinned spec.runtimeVersion yields a distinct serving image and the
// revision hash reflects it); otherwise the device default (distinct CPU/CUDA
// images). It lets the test assert that a cuda DM without spec.image prefetches
// with the CPU default rather than the resolved serving (CUDA) image.
func fakeImageFor(p engine.Params) string {
	if p.Image != "" {
		return p.Image
	}
	// A non-default pinned version yields a version-specific tag so the revision
	// hash reflects it. The engine default version (or an unset version) maps to
	// the plain device-default image, matching the real engine where the default
	// version IS the default image — so recording and reusing the default version
	// under Pinned never changes the resolved image or the hash.
	if p.RuntimeVersion != "" && p.RuntimeVersion != ollaya.DefaultRuntimeVersion {
		img := "ghcr.io/ollaya-dev/ollaya:" + p.RuntimeVersion
		if p.Device == engine.DeviceCUDA {
			img += "-cuda"
		}
		return img
	}
	if p.Device == engine.DeviceCUDA {
		return fakeImageCUDA
	}
	return fakeImage
}

func (f *fakeEngine) ServingPodSpec(p engine.Params) corev1.PodSpec {
	img := fakeImageFor(p)
	return corev1.PodSpec{
		Containers: []corev1.Container{{
			Name:      "ollaya",
			Image:     img,
			Ports:     []corev1.ContainerPort{{Name: "http", ContainerPort: fakeServingPort}},
			Resources: p.Resources,
			VolumeMounts: []corev1.VolumeMount{
				{Name: "models", MountPath: "/models", SubPath: p.StoreSubPath, ReadOnly: true},
			},
		}},
		Volumes: []corev1.Volume{{
			Name: "models",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: p.CacheClaimName,
					ReadOnly:  true,
				},
			},
		}},
	}
}

func (f *fakeEngine) PrefetchJobSpec(p engine.Params) batchv1.JobSpec {
	// Mirror the real engine: pulling needs no GPU, so the prefetch image is the
	// CPU default unless p.Image overrides it — never the device-aware image.
	img := fakeImage
	if p.Image != "" {
		img = p.Image
	}
	return batchv1.JobSpec{
		Template: corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyNever,
				Containers: []corev1.Container{{
					Name:  "prefetch",
					Image: img,
					Env:   fakeJobEnv(p),
				}},
			},
		},
	}
}

// fakeJobEnv mirrors the real engine's download-token injection so envtests can
// assert Job env content without using the real engine.
func fakeJobEnv(p engine.Params) []corev1.EnvVar {
	if p.DownloadToken == nil {
		return nil
	}
	ref := p.DownloadToken.DeepCopy()
	return []corev1.EnvVar{{
		Name:      "OLLAYA_HF_TOKEN",
		ValueFrom: &corev1.EnvVarSource{SecretKeyRef: ref},
	}}
}

func (f *fakeEngine) ServicePort() int32 { return fakeServingPort }

func (f *fakeEngine) Inspect(_ context.Context, _, _ string) ([]engine.Loaded, error) {
	return nil, nil
}

func (f *fakeEngine) Warmup(_ context.Context, _, _, _ string) error { return nil }

// --- fake prober ---------------------------------------------------------

// fakeProber returns a fixed Loaded (or error) for every Pod, so tests control
// digest/device without a running runtime. Fields are mutable so a test can
// change the reported digest across a rollout. It counts probe calls.
type fakeProber struct {
	mu     sync.Mutex
	loaded engine.Loaded
	err    error
	calls  int
	// unpinned makes the fake report the model as loaded but NOT pinned. By
	// default a healthy loaded model is pinned (keep_alive -1), as the operator's
	// Warmup makes it; a spec that exercises the NotPinned rule sets this.
	unpinned bool
}

// result is what the fake runtime reports: p.loaded, pinned unless unpinned.
func (p *fakeProber) result() engine.Loaded {
	l := p.loaded
	l.Pinned = !p.unpinned
	return l
}

func (p *fakeProber) Probe(
	_ context.Context,
	_ *corev1.Pod,
	_ engine.Engine,
	_, _ string,
) (engine.Loaded, error) {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	if p.err != nil {
		return engine.Loaded{}, p.err
	}
	return p.result(), nil
}

func (p *fakeProber) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// --- helpers -------------------------------------------------------------

var _ = Describe("DecisionModel Controller", func() {
	const (
		digest = defaultDigest
		model  = "laya:en"
	)

	var (
		ctx       context.Context
		namespace string
		nsCounter int
	)

	// int32Ptr is a small helper for spec.replicas.
	int32Ptr := func(v int32) *int32 { return &v }

	newReconciler := func(eng *fakeEngine, prober Prober) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client:    k8sClient,
			APIReader: k8sClient,
			Scheme:    k8sClient.Scheme(),
			Engines:   map[string]engine.Engine{"ollaya": eng},
			Prober:    prober,
			Recorder:  events.NewFakeRecorder(64),
		}
	}

	// drainEvents returns all buffered event strings from a reconciler's FakeRecorder.
	drainEvents := func(r *DecisionModelReconciler) []string {
		fr := r.Recorder.(*events.FakeRecorder)
		var out []string
		for {
			select {
			case e := <-fr.Events:
				out = append(out, e)
			default:
				return out
			}
		}
	}

	hasEvent := func(events []string, substr string) bool {
		for _, e := range events {
			if strings.Contains(e, substr) {
				return true
			}
		}
		return false
	}

	reconcileOnce := func(r *DecisionModelReconciler, name string) {
		_, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: namespace, Name: name},
		})
		Expect(err).NotTo(HaveOccurred())
	}

	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}

	// createReadyPod fabricates a serving Pod for a revision with the given
	// labels, a PodIP and ContainersReady=True (kubelet is absent in envtest).
	createReadyPod := func(dmName, rev, podName string) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      podName,
				Labels: map[string]string{
					decisionmodelv1alpha1.LabelName:     dmName,
					decisionmodelv1alpha1.LabelRevision: rev,
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}},
			},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.PodIP = "10.0.0.10"
		pod.Status.Conditions = []corev1.PodCondition{{
			Type:   corev1.ContainersReady,
			Status: corev1.ConditionTrue,
		}}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}

	markJob := func(dmName, rev string, condType batchv1.JobConditionType) {
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Namespace: namespace, Name: dmName + "-prefetch-" + rev,
		}, job)).To(Succeed())
		now := metav1.Now()
		job.Status.StartTime = &now
		switch condType {
		case batchv1.JobComplete:
			// The apiserver requires SuccessCriteriaMet=True and a completionTime
			// before it accepts Complete=True.
			job.Status.CompletionTime = &now
			job.Status.Conditions = []batchv1.JobCondition{
				{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
				{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
			}
		case batchv1.JobFailed:
			job.Status.Conditions = []batchv1.JobCondition{
				{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue},
				{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
			}
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}

	// setPodContainersReadyLTT sets the ContainersReady condition's transition
	// time (simulating a container restart that became ready again).
	setPodContainersReadyLTT := func(podName string, t metav1.Time) {
		pod := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, pod)).To(Succeed())
		found := false
		for i := range pod.Status.Conditions {
			if pod.Status.Conditions[i].Type == corev1.ContainersReady {
				pod.Status.Conditions[i].LastTransitionTime = t
				found = true
			}
		}
		if !found {
			pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
				Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: t,
			})
		}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}

	podGate := func(podName string) *corev1.PodCondition {
		pod := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, pod)).To(Succeed())
		for i := range pod.Status.Conditions {
			if string(pod.Status.Conditions[i].Type) == decisionmodelv1alpha1.ModelReadyGate {
				return &pod.Status.Conditions[i]
			}
		}
		return nil
	}
	revOf := func(dm *decisionmodelv1alpha1.DecisionModel) string {
		return RevisionHash(dm.Spec, digest, fakeImage)
	}

	createDM := func(name string, mutate func(*decisionmodelv1alpha1.DecisionModel)) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine:   "ollaya",
				Model:    model,
				Device:   "cpu",
				Replicas: int32Ptr(1),
				// These scenarios assert the old revision is collected shortly after
				// promotion; disable the stabilization window so the 5m default does
				// not keep it alive. (The window has its own dedicated specs.)
				Rollout: zeroStabilizationRollout(),
			},
		}
		if mutate != nil {
			mutate(dm)
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
		return dm
	}

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = fmt.Sprintf("dm-test-%d", nsCounter)
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
	})

	// Scenario 1: New DM -> PVC + prefetch Job, phase Caching, Resolved=True, digest in candidate.
	It("creates PVC and prefetch Job and enters Caching", func() {
		eng := newFakeEngine()
		r := newReconciler(eng, &fakeProber{})
		createDM("s1", nil)

		reconcileOnce(r, "s1")

		dm := getDM("s1")
		rev := revOf(dm)
		pvc := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "s1-store-" + rev}, pvc)).To(Succeed())

		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "s1-prefetch-" + rev}, job)).To(Succeed())

		Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseCaching))
		Expect(meta.IsStatusConditionTrue(dm.Status.Conditions, decisionmodelv1alpha1.ConditionResolved)).To(BeTrue())
		Expect(dm.Status.CandidateRevision).NotTo(BeNil())
		Expect(dm.Status.CandidateRevision.Digest).To(Equal(digest))
	})

	// Scenario 1b: spec.scheduling also applies to the prefetch Job Pod, so it can
	// run on tainted/dedicated pools and pins the PVC to the serving node's zone.
	It("applies spec.scheduling to the prefetch Job", func() {
		eng := newFakeEngine()
		r := newReconciler(eng, &fakeProber{})
		sched := &decisionmodelv1alpha1.SchedulingSpec{
			NodeSelector: map[string]string{"pool": "gpu"},
			Tolerations:  []corev1.Toleration{{Key: "gpu", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}},
		}
		createDM("s1b", func(dm *decisionmodelv1alpha1.DecisionModel) { dm.Spec.Scheduling = sched })

		reconcileOnce(r, "s1b")

		rev := revOf(getDM("s1b"))
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "s1b-prefetch-" + rev}, job)).To(Succeed())
		Expect(job.Spec.Template.Spec.NodeSelector).To(Equal(sched.NodeSelector))
		Expect(job.Spec.Template.Spec.Tolerations).To(Equal(sched.Tolerations))
		// Shared SELinux level (store relabel on SELinux-enforcing nodes).
		Expect(job.Spec.Template.Spec.SecurityContext).NotTo(BeNil())
		Expect(job.Spec.Template.Spec.SecurityContext.SELinuxOptions).To(Equal(
			&corev1.SELinuxOptions{Level: selinuxLevel(getDM("s1b"))}))
	})

	// Scenario 2: Job Complete -> Deployment with readiness gate, phase Starting.
	It("creates the serving Deployment with a readiness gate once cached", func() {
		eng := newFakeEngine()
		r := newReconciler(eng, &fakeProber{})
		createDM("s2", nil)

		reconcileOnce(r, "s2")
		rev := revOf(getDM("s2"))
		markJob("s2", rev, batchv1.JobComplete)
		reconcileOnce(r, "s2")

		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "s2-" + rev}, dep)).To(Succeed())
		Expect(dep.Labels[decisionmodelv1alpha1.LabelRevision]).To(Equal(rev))
		Expect(dep.Spec.Template.Spec.ReadinessGates).To(ContainElement(corev1.PodReadinessGate{
			ConditionType: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate),
		}))
		// Same SELinux level as the prefetch Job, so the relabelled store is readable.
		Expect(dep.Spec.Template.Spec.SecurityContext.SELinuxOptions).To(Equal(
			&corev1.SELinuxOptions{Level: selinuxLevel(getDM("s2"))}))

		dm := getDM("s2")
		Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseStarting))
		Expect(meta.IsStatusConditionTrue(dm.Status.Conditions, decisionmodelv1alpha1.ConditionCached)).To(BeTrue())
	})

	// Scenario 3: Pods probed OK -> gate True, Service selects rev, phase Ready, stable set, candidate nil.
	It("promotes when candidate Pods are model-ready", func() {
		eng := newFakeEngine()
		r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{
			Name: model, Digest: digest, Device: "cpu", Precision: "F32",
		}})
		createDM("s3", nil)

		reconcileOnce(r, "s3")
		rev := revOf(getDM("s3"))
		markJob("s3", rev, batchv1.JobComplete)
		reconcileOnce(r, "s3")
		createReadyPod("s3", rev, "s3-pod-0")
		reconcileOnce(r, "s3") // probe -> gate True
		reconcileOnce(r, "s3") // promote

		dm := getDM("s3")
		Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseReady))
		Expect(dm.Status.StableRevision).NotTo(BeNil())
		Expect(dm.Status.StableRevision.Hash).To(Equal(rev))
		Expect(dm.Status.StableRevision.Precision).To(Equal("F32"))
		Expect(dm.Status.CandidateRevision).To(BeNil())
		Expect(dm.Status.Replicas.ModelReady).To(Equal(int32(1)))
		Expect(meta.IsStatusConditionTrue(dm.Status.Conditions, decisionmodelv1alpha1.ConditionReady)).To(BeTrue())

		svc := &corev1.Service{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "s3"}, svc)).To(Succeed())
		Expect(svc.Spec.Selector[decisionmodelv1alpha1.LabelRevision]).To(Equal(rev))
	})

	// Scenario 4: Probe device mismatch -> gate False reason DeviceMismatch, no promotion.
	It("does not promote on device mismatch", func() {
		eng := newFakeEngine()
		r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{
			Name: model, Digest: digest, Device: "cuda", Precision: "F16",
		}})
		createDM("s4", nil)

		reconcileOnce(r, "s4")
		rev := revOf(getDM("s4"))
		markJob("s4", rev, batchv1.JobComplete)
		reconcileOnce(r, "s4")
		createReadyPod("s4", rev, "s4-pod-0")
		reconcileOnce(r, "s4")

		pod := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "s4-pod-0"}, pod)).To(Succeed())
		var gate *corev1.PodCondition
		for i := range pod.Status.Conditions {
			if string(pod.Status.Conditions[i].Type) == decisionmodelv1alpha1.ModelReadyGate {
				gate = &pod.Status.Conditions[i]
			}
		}
		Expect(gate).NotTo(BeNil())
		Expect(gate.Status).To(Equal(corev1.ConditionFalse))
		Expect(gate.Reason).To(Equal(reasonDeviceMismatch))

		dm := getDM("s4")
		Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseStarting))
		Expect(dm.Status.StableRevision).To(BeNil())
	})

	// Scenario 5: Spec model change on a Ready DM -> new candidate; old Service selector unchanged
	// until promotion; after promotion old Deployment deleted.
	It("rolls out a new revision on model change and cleans up the old one", func() {
		eng := newFakeEngine()
		prober := &fakeProber{loaded: engine.Loaded{
			Name: model, Digest: digest, Device: "cpu", Precision: "F32",
		}}
		r := newReconciler(eng, prober)
		createDM("s5", nil)

		// Bring s5 to Ready on the first revision.
		reconcileOnce(r, "s5")
		rev1 := revOf(getDM("s5"))
		markJob("s5", rev1, batchv1.JobComplete)
		reconcileOnce(r, "s5")
		createReadyPod("s5", rev1, "s5-pod-0")
		reconcileOnce(r, "s5")
		reconcileOnce(r, "s5")
		Expect(getDM("s5").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseReady))

		// Change the model -> new digest -> new revision. The runtime now reports
		// the new digest for the new revision's Pods.
		const digest2 = "def4560000000000000000000000000000000000000000000000000000000000"
		eng.digest = digest2
		prober.loaded.Digest = digest2
		Expect(updateDM(ctx, namespace, "s5", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "kev:en"
		})).To(Succeed())

		reconcileOnce(r, "s5") // resolve + cache new rev
		dm := getDM("s5")
		rev2 := RevisionHash(dm.Spec, digest2, fakeImage)
		Expect(rev2).NotTo(Equal(rev1))
		Expect(dm.Status.CandidateRevision).NotTo(BeNil())
		Expect(dm.Status.CandidateRevision.Hash).To(Equal(rev2))

		// Old Service selector still points at rev1 until the candidate is promoted.
		svc := &corev1.Service{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "s5"}, svc)).To(Succeed())
		Expect(svc.Spec.Selector[decisionmodelv1alpha1.LabelRevision]).To(Equal(rev1))

		markJob("s5", rev2, batchv1.JobComplete)
		reconcileOnce(r, "s5")
		createReadyPod("s5", rev2, "s5-pod-1")
		reconcileOnce(r, "s5") // probe
		reconcileOnce(r, "s5") // promote (GC of old revision deferred by promoteGrace)

		dm = getDM("s5")
		Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseReady))
		Expect(dm.Status.StableRevision.Hash).To(Equal(rev2))

		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "s5"}, svc)).To(Succeed())
		Expect(svc.Spec.Selector[decisionmodelv1alpha1.LabelRevision]).To(Equal(rev2))

		// Advance past the promote grace so the stable path GCs the old revision.
		r.Now = func() time.Time { return time.Now().Add(2 * promoteGrace) }
		reconcileOnce(r, "s5")

		// Old Deployment gone.
		oldDep := &appsv1.Deployment{}
		err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "s5-" + rev1}, oldDep)
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
	})

	// Scenario 6: Replicas change -> same rev, Deployment replicas updated, no new Job.
	It("updates replicas in place without a new revision or Job", func() {
		eng := newFakeEngine()
		r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{
			Name: model, Digest: digest, Device: "cpu", Precision: "F32",
		}})
		createDM("s6", nil)

		reconcileOnce(r, "s6")
		rev := revOf(getDM("s6"))
		markJob("s6", rev, batchv1.JobComplete)
		reconcileOnce(r, "s6")
		createReadyPod("s6", rev, "s6-pod-0")
		reconcileOnce(r, "s6")
		reconcileOnce(r, "s6")
		Expect(getDM("s6").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseReady))

		// Bump replicas.
		Expect(updateDM(ctx, namespace, "s6", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Replicas = int32Ptr(3)
		})).To(Succeed())
		reconcileOnce(r, "s6")

		// Same revision, Deployment replicas updated.
		Expect(RevisionHash(getDM("s6").Spec, digest, fakeImage)).To(Equal(rev))
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "s6-" + rev}, dep)).To(Succeed())
		Expect(*dep.Spec.Replicas).To(Equal(int32(3)))

		// No second Job (still exactly one prefetch Job for this DM).
		var jobs batchv1.JobList
		Expect(k8sClient.List(ctx, &jobs, client.InNamespace(namespace))).To(Succeed())
		Expect(jobs.Items).To(HaveLen(1))
	})

	// Scenario 7: Prefetch failed with stable present -> RolledBack, stable still selected;
	// without stable -> Failed.
	It("fails when prefetch fails and there is no stable revision", func() {
		eng := newFakeEngine()
		r := newReconciler(eng, &fakeProber{})
		createDM("s7", nil)

		reconcileOnce(r, "s7")
		rev := revOf(getDM("s7"))
		markJob("s7", rev, batchv1.JobFailed)
		reconcileOnce(r, "s7")

		dm := getDM("s7")
		Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseFailed))
		Expect(meta.IsStatusConditionTrue(dm.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded)).To(BeTrue())
		cond := meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded)
		Expect(cond.Reason).To(Equal(reasonPrefetchFailed))
	})

	It("rolls back to the stable revision when a new revision's prefetch fails", func() {
		eng := newFakeEngine()
		r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{
			Name: model, Digest: digest, Device: "cpu", Precision: "F32",
		}})
		createDM("s7b", nil)

		// Reach Ready on rev1.
		reconcileOnce(r, "s7b")
		rev1 := revOf(getDM("s7b"))
		markJob("s7b", rev1, batchv1.JobComplete)
		reconcileOnce(r, "s7b")
		createReadyPod("s7b", rev1, "s7b-pod-0")
		reconcileOnce(r, "s7b")
		reconcileOnce(r, "s7b")
		Expect(getDM("s7b").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseReady))

		// New revision whose prefetch fails.
		const digest2 = "aaa1110000000000000000000000000000000000000000000000000000000000"
		eng.digest = digest2
		Expect(updateDM(ctx, namespace, "s7b", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "jevk5:en"
		})).To(Succeed())
		reconcileOnce(r, "s7b")
		rev2 := RevisionHash(getDM("s7b").Spec, digest2, fakeImage)
		markJob("s7b", rev2, batchv1.JobFailed)
		reconcileOnce(r, "s7b")

		dm := getDM("s7b")
		Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseRolledBack))
		Expect(dm.Status.StableRevision).NotTo(BeNil())
		Expect(dm.Status.StableRevision.Hash).To(Equal(rev1))

		// Stable Service selector unchanged; candidate Deployment removed.
		svc := &corev1.Service{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "s7b"}, svc)).To(Succeed())
		Expect(svc.Spec.Selector[decisionmodelv1alpha1.LabelRevision]).To(Equal(rev1))
	})

	// Scenario 8: spec.digest set -> Resolve not called.
	It("does not call Resolve when spec.digest is set", func() {
		eng := newFakeEngine()
		r := newReconciler(eng, &fakeProber{})
		createDM("s8", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Digest = digest
		})

		reconcileOnce(r, "s8")

		Expect(eng.resolveCount()).To(Equal(0))
		dm := getDM("s8")
		Expect(dm.Status.CandidateRevision).NotTo(BeNil())
		Expect(dm.Status.CandidateRevision.Digest).To(Equal(digest))
	})

	// Unknown engine -> Failed with Ready=False reason UnknownEngine.
	// The CRD enum blocks an invalid spec.engine at create time, so the runtime
	// guard is exercised via a reconciler whose Engines map lacks the engine.
	It("fails on an unknown engine", func() {
		r := &DecisionModelReconciler{
			Client:  k8sClient,
			Scheme:  k8sClient.Scheme(),
			Engines: map[string]engine.Engine{}, // no "ollaya" registered
			Prober:  &fakeProber{},
		}
		createDM("s9", nil)

		reconcileOnce(r, "s9")

		dm := getDM("s9")
		Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseFailed))
		cond := meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionReady)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal(reasonUnknownEngine))
	})

	// --- regression tests ------------------------------------------

	// toReady drives a DM to Ready on its first revision with one Pod.
	toReady := func(r *DecisionModelReconciler, name, podName string) string {
		reconcileOnce(r, name)
		rev := revOf(getDM(name))
		markJob(name, rev, batchv1.JobComplete)
		reconcileOnce(r, name)
		createReadyPod(name, rev, podName)
		reconcileOnce(r, name) // probe
		reconcileOnce(r, name) // promote
		Expect(getDM(name).Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseReady))
		return rev
	}

	// a newly added Pod of the STABLE revision must be probed and gated.
	It("probes newly added stable-revision Pods", func() {
		eng := newFakeEngine()
		r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{
			Name: model, Digest: digest, Device: "cpu", Precision: "F32",
		}})
		createDM("t1", func(dm *decisionmodelv1alpha1.DecisionModel) { dm.Spec.Replicas = int32Ptr(2) })

		// First Pod ready -> not yet promoted (needs 2), so promote after both.
		reconcileOnce(r, "t1")
		rev := revOf(getDM("t1"))
		markJob("t1", rev, batchv1.JobComplete)
		reconcileOnce(r, "t1")
		createReadyPod("t1", rev, "t1-pod-0")
		createReadyPod("t1", rev, "t1-pod-1")
		reconcileOnce(r, "t1") // probe both
		reconcileOnce(r, "t1") // promote
		Expect(getDM("t1").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseReady))
		Expect(getDM("t1").Status.Replicas.ModelReady).To(Equal(int32(2)))

		// Now a replacement/scale Pod appears on the stable revision.
		createReadyPod("t1", rev, "t1-pod-2")
		reconcileOnce(r, "t1")

		gate := podGate("t1-pod-2")
		Expect(gate).NotTo(BeNil())
		Expect(gate.Status).To(Equal(corev1.ConditionTrue))
		// A Pod whose gate was patched in THIS reconcile is not counted yet: the
		// Service only moves onto Pods Kubernetes already reports Ready.
		Expect(getDM("t1").Status.Replicas.ModelReady).To(Equal(int32(2)))
		// The next reconcile (brought in production by the PodReady transition
		// event) counts it.
		reconcileOnce(r, "t1")
		Expect(getDM("t1").Status.Replicas.ModelReady).To(Equal(int32(3)))
	})

	// a gate-True Pod is re-probed only after ContainersReady moves past the gate.
	It("re-probes a Pod only after its containers restart", func() {
		eng := newFakeEngine()
		prober := &fakeProber{loaded: engine.Loaded{
			Name: model, Digest: digest, Device: "cpu", Precision: "F32",
		}}
		r := newReconciler(eng, prober)
		createDM("t2", nil)
		rev := toReady(r, "t2", "t2-pod-0")
		_ = rev

		callsAfterReady := prober.callCount()

		// Reconcile again without any restart -> no extra probe.
		reconcileOnce(r, "t2")
		Expect(prober.callCount()).To(Equal(callsAfterReady), "should not re-probe a stable gate-True Pod")

		// Simulate a container restart that became ready after the gate was set.
		setPodContainersReadyLTT("t2-pod-0", metav1.NewTime(metav1.Now().Add(time.Hour)))
		reconcileOnce(r, "t2")
		Expect(prober.callCount()).To(BeNumerically(">", callsAfterReady), "should re-probe after container restart")
	})

	// a failed revision is not retried; a spec change starts a new one.
	It("does not retry a failed revision until the spec changes", func() {
		eng := newFakeEngine()
		r := newReconciler(eng, &fakeProber{})
		createDM("t3", nil)

		reconcileOnce(r, "t3")
		rev := revOf(getDM("t3"))
		markJob("t3", rev, batchv1.JobFailed)
		reconcileOnce(r, "t3")
		Expect(getDM("t3").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseFailed))
		Expect(getDM("t3").Status.FailedRevision).NotTo(BeNil())
		Expect(getDM("t3").Status.FailedRevision.Hash).To(Equal(rev))

		// Reconcile again: the failed revision's Job must NOT be recreated.
		reconcileOnce(r, "t3")
		job := &batchv1.Job{}
		err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "t3-prefetch-" + rev}, job)
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "failed revision Job must not be recreated")

		// Change the model -> new revision -> proceeds (new Job created).
		const digest2 = "bbb2220000000000000000000000000000000000000000000000000000000000"
		eng.digest = digest2
		Expect(updateDM(ctx, namespace, "t3", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "kev:en"
		})).To(Succeed())
		reconcileOnce(r, "t3")

		rev2 := RevisionHash(getDM("t3").Spec, digest2, fakeImage)
		Expect(rev2).NotTo(Equal(rev))
		newJob := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "t3-prefetch-" + rev2}, newJob)).To(Succeed())
	})

	// Caching longer than cacheTimeout with no stable -> CacheTimeout Failed.
	It("times out a stuck Caching phase", func() {
		eng := newFakeEngine()
		clock := metav1.Now().Time
		r := newReconciler(eng, &fakeProber{})
		r.Now = func() time.Time { return clock }
		createDM("t4", nil)

		reconcileOnce(r, "t4") // enters Caching, phaseTransitionTime = clock
		Expect(getDM("t4").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseCaching))

		// Advance the clock beyond the cache timeout; Job still not complete.
		clock = clock.Add(cacheTimeout + time.Minute)
		reconcileOnce(r, "t4")

		dm := getDM("t4")
		Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseFailed))
		cond := meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Reason).To(Equal(reasonCacheTimeout))
	})

	// Starting longer than startTimeout with no stable -> StartTimeout Failed.
	It("times out a stuck Starting phase", func() {
		eng := newFakeEngine()
		clock := metav1.Now().Time
		// Prober reports a mismatch so the candidate never becomes ready.
		r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{
			Name: model, Digest: digest, Device: "cuda",
		}})
		r.Now = func() time.Time { return clock }
		createDM("t5", nil)

		reconcileOnce(r, "t5")
		rev := revOf(getDM("t5"))
		markJob("t5", rev, batchv1.JobComplete)
		reconcileOnce(r, "t5") // deployment + probe -> Starting
		createReadyPod("t5", rev, "t5-pod-0")
		reconcileOnce(r, "t5") // probe mismatch, phase Starting set
		Expect(getDM("t5").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseStarting))

		clock = clock.Add(startTimeout + time.Minute)
		reconcileOnce(r, "t5")

		dm := getDM("t5")
		Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseFailed))
		cond := meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Reason).To(Equal(reasonStartTimeout))
	})

	// spec.model must carry an explicit tag in its last path segment.
	It("rejects a model without an explicit tag and accepts a tagged one", func() {
		mk := func(name, mdl string) *decisionmodelv1alpha1.DecisionModel {
			return &decisionmodelv1alpha1.DecisionModel{
				ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
				Spec: decisionmodelv1alpha1.DecisionModelSpec{
					Engine: "ollaya", Model: mdl, Device: "cpu", Replicas: int32Ptr(1),
				},
			}
		}

		// Invalid: bare names / no tag in the last segment.
		for i, bad := range []string{"laya", "localhost:5000/ns/m", "acme/triage"} {
			err := k8sClient.Create(ctx, mk(fmt.Sprintf("bad-%d", i), bad))
			Expect(err).To(HaveOccurred(), "expected %q to be rejected", bad)
			Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected an Invalid error for %q, got %v", bad, err)
		}

		// Valid: explicit tag on the last segment.
		for i, good := range []string{"laya:en", "acme/triage:v1", "localhost:5000/ns/m:t"} {
			Expect(k8sClient.Create(ctx, mk(fmt.Sprintf("good-%d", i), good))).
				To(Succeed(), "expected %q to be accepted", good)
		}
	})

	// --- tests -----------------------------------------------------

	// Events are emitted on transitions through a full rollout.
	It("emits lifecycle Events on transitions", func() {
		eng := newFakeEngine()
		r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{
			Name: model, Digest: digest, Device: "cpu", Precision: "F32",
		}})
		createDM("e1", nil)

		reconcileOnce(r, "e1") // Resolved + PrefetchStarted (+ Caching)
		ev := drainEvents(r)
		Expect(hasEvent(ev, eventResolved)).To(BeTrue(), "expected Resolved event, got %v", ev)
		Expect(hasEvent(ev, eventPrefetchStarted)).To(BeTrue(), "expected PrefetchStarted event, got %v", ev)

		rev := revOf(getDM("e1"))
		markJob("e1", rev, batchv1.JobComplete)
		reconcileOnce(r, "e1") // Cached + RevisionStarting
		ev = drainEvents(r)
		Expect(hasEvent(ev, eventCached)).To(BeTrue(), "expected Cached event, got %v", ev)
		Expect(hasEvent(ev, eventRevisionStarting)).To(BeTrue(), "expected RevisionStarting event, got %v", ev)

		createReadyPod("e1", rev, "e1-pod-0")
		reconcileOnce(r, "e1") // probe
		reconcileOnce(r, "e1") // promote
		ev = drainEvents(r)
		Expect(hasEvent(ev, eventPromoted)).To(BeTrue(), "expected Promoted event, got %v", ev)

		// A steady-state reconcile of a Ready DM emits no new lifecycle events.
		reconcileOnce(r, "e1")
		Expect(drainEvents(r)).To(BeEmpty())
	})

	// A device mismatch emits a Warning ProbeMismatch once per gate transition.
	It("emits a ProbeMismatch Warning once per gate transition", func() {
		eng := newFakeEngine()
		r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{
			Name: model, Digest: digest, Device: "cuda",
		}})
		createDM("e2", nil)

		reconcileOnce(r, "e2")
		rev := revOf(getDM("e2"))
		markJob("e2", rev, batchv1.JobComplete)
		reconcileOnce(r, "e2")
		createReadyPod("e2", rev, "e2-pod-0")
		reconcileOnce(r, "e2") // probe -> mismatch, gate False (transition)
		ev := drainEvents(r)
		Expect(hasEvent(ev, eventProbeMismatch)).To(BeTrue(), "expected ProbeMismatch event, got %v", ev)
		Expect(hasEvent(ev, reasonDeviceMismatch)).To(BeTrue())

		// Re-probe with the same mismatch: gate stays False, no new event.
		reconcileOnce(r, "e2")
		Expect(hasEvent(drainEvents(r), eventProbeMismatch)).To(BeFalse(),
			"ProbeMismatch must fire only on gate transition")
	})

	// Cache guard: replicas>1 with a default RWO PVC -> Degraded CacheNotShareable.
	It("flags CacheNotShareable for replicas>1 on an RWO store", func() {
		eng := newFakeEngine()
		r := newReconciler(eng, &fakeProber{})
		createDM("c1", func(dm *decisionmodelv1alpha1.DecisionModel) { dm.Spec.Replicas = int32Ptr(2) })

		reconcileOnce(r, "c1")

		dm := getDM("c1")
		cond := meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		Expect(cond.Reason).To(Equal(reasonCacheNotShareable))

		pvc := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "c1-store-" + revOf(dm)}, pvc)).To(Succeed())
		Expect(pvc.Spec.AccessModes).To(Equal([]corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}))
	})

	// Cache guard: replicas>1 with an RWX store -> no Degraded from the cache guard.
	It("does not flag CacheNotShareable when the store is ReadWriteMany", func() {
		eng := newFakeEngine()
		r := newReconciler(eng, &fakeProber{})
		createDM("c2", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Replicas = int32Ptr(2)
			dm.Spec.Cache = &decisionmodelv1alpha1.CacheSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			}
		})

		reconcileOnce(r, "c2")

		pvc := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "c2-store-" + revOf(getDM("c2"))}, pvc)).To(Succeed())
		Expect(pvc.Spec.AccessModes).To(ContainElement(corev1.ReadWriteMany))

		dm := getDM("c2")
		cond := meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded)
		// Either no Degraded condition yet, or it is not the cache-not-shareable reason.
		if cond != nil {
			Expect(cond.Reason).NotTo(Equal(reasonCacheNotShareable))
		}
	})

	// Cache guard: changing accessModes on an existing PVC -> CacheSpecImmutable, PVC unchanged.
	It("flags CacheSpecImmutable and does not recreate the PVC on accessModes change", func() {
		eng := newFakeEngine()
		r := newReconciler(eng, &fakeProber{})
		// Create with default RWO PVC.
		createDM("c3", nil)
		reconcileOnce(r, "c3")
		c3rev := revOf(getDM("c3"))
		pvc := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "c3-store-" + c3rev}, pvc)).To(Succeed())
		originalUID := pvc.UID
		Expect(pvc.Spec.AccessModes).To(Equal([]corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}))

		// Now ask for RWX in the spec (differs from the immutable PVC).
		Expect(updateDM(ctx, namespace, "c3", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Cache = &decisionmodelv1alpha1.CacheSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			}
		})).To(Succeed())
		reconcileOnce(r, "c3")

		dm := getDM("c3")
		cond := meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Reason).To(Equal(reasonCacheSpecImmutable))

		// PVC unchanged (same UID, still RWO).
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "c3-store-" + c3rev}, pvc)).To(Succeed())
		Expect(pvc.UID).To(Equal(originalUID))
		Expect(pvc.Spec.AccessModes).To(Equal([]corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}))
	})

	// a successful promotion clears a prior rollout Degraded (not cache).
	It("clears a rollout Degraded on the next successful promotion", func() {
		eng := newFakeEngine()
		prober := &fakeProber{loaded: engine.Loaded{
			Name: model, Digest: digest, Device: "cpu", Precision: "F32",
		}}
		r := newReconciler(eng, prober)
		createDM("d1", nil)

		// First revision fails to prefetch -> Failed + Degraded=True.
		reconcileOnce(r, "d1")
		rev1 := revOf(getDM("d1"))
		markJob("d1", rev1, batchv1.JobFailed)
		reconcileOnce(r, "d1")
		deg := meta.FindStatusCondition(getDM("d1").Status.Conditions, decisionmodelv1alpha1.ConditionDegraded)
		Expect(deg).NotTo(BeNil())
		Expect(deg.Status).To(Equal(metav1.ConditionTrue))
		Expect(deg.Reason).To(Equal(reasonPrefetchFailed))

		// New revision rolls out successfully.
		const digest2 = "ccc3330000000000000000000000000000000000000000000000000000000000"
		eng.digest = digest2
		prober.loaded.Digest = digest2
		Expect(updateDM(ctx, namespace, "d1", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "kev:en"
		})).To(Succeed())

		reconcileOnce(r, "d1")
		rev2 := RevisionHash(getDM("d1").Spec, digest2, fakeImage)
		markJob("d1", rev2, batchv1.JobComplete)
		reconcileOnce(r, "d1")
		createReadyPod("d1", rev2, "d1-pod-0")
		reconcileOnce(r, "d1") // probe
		reconcileOnce(r, "d1") // promote

		dm := getDM("d1")
		Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseReady))
		deg = meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded)
		Expect(deg).NotTo(BeNil())
		Expect(deg.Status).To(Equal(metav1.ConditionFalse), "rollout Degraded must clear on successful promotion")
		Expect(dm.Status.FailedRevision).To(BeNil())
	})

	// revert manual drift of owned serving Deployments and Services.
	//
	// The desired-spec hash annotation alone covers only the desired values, so
	// an out-of-band change to the live object (e.g. `kubectl scale
	// --replicas=0`, a removed readiness gate, an edited Service selector) would
	// otherwise never be reconciled back. These tests drive a DM to Ready, then
	// mutate the live object and assert the next reconcile restores it, while a
	// no-drift reconcile stays a no-op (resourceVersion unchanged).
	Context("drift correction on owned objects", func() {
		// promote drives a fresh DM to phase Ready and returns its revision name.
		promote := func(r *DecisionModelReconciler, name string) string {
			createDM(name, nil)
			reconcileOnce(r, name)
			rev := revOf(getDM(name))
			markJob(name, rev, batchv1.JobComplete)
			reconcileOnce(r, name)
			createReadyPod(name, rev, name+"-pod-0")
			reconcileOnce(r, name) // probe -> gate True
			reconcileOnce(r, name) // promote
			Expect(getDM(name).Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseReady))
			return rev
		}

		readyReconciler := func() *DecisionModelReconciler {
			return newReconciler(newFakeEngine(), &fakeProber{loaded: engine.Loaded{
				Name: model, Digest: digest, Device: "cpu", Precision: "F32",
			}})
		}

		getDep := func(name, rev string) *appsv1.Deployment {
			dep := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: namespace, Name: name + "-" + rev,
			}, dep)).To(Succeed())
			return dep
		}

		getSvc := func(name string) *corev1.Service {
			svc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, svc)).To(Succeed())
			return svc
		}

		hasReadinessGate := func(dep *appsv1.Deployment) bool {
			for _, g := range dep.Spec.Template.Spec.ReadinessGates {
				if string(g.ConditionType) == decisionmodelv1alpha1.ModelReadyGate {
					return true
				}
			}
			return false
		}

		It("restores spec.replicas after a manual scale to 0", func() {
			r := readyReconciler()
			rev := promote(r, "drift1")

			// Manual `kubectl scale --replicas=0` on the serving Deployment.
			Expect(k8sClient.Update(ctx, func() *appsv1.Deployment {
				dep := getDep("drift1", rev)
				dep.Spec.Replicas = int32Ptr(0)
				return dep
			}())).To(Succeed())
			Expect(*getDep("drift1", rev).Spec.Replicas).To(Equal(int32(0)))

			reconcileOnce(r, "drift1")
			Expect(*getDep("drift1", rev).Spec.Replicas).To(Equal(int32(1)),
				"manual scale to 0 must be reverted to the desired replica count")
		})

		It("restores the readiness gate after it is removed by hand", func() {
			r := readyReconciler()
			rev := promote(r, "drift2")
			Expect(hasReadinessGate(getDep("drift2", rev))).To(BeTrue())

			// Strip the readiness gate from the live template.
			Expect(k8sClient.Update(ctx, func() *appsv1.Deployment {
				dep := getDep("drift2", rev)
				dep.Spec.Template.Spec.ReadinessGates = nil
				return dep
			}())).To(Succeed())
			Expect(hasReadinessGate(getDep("drift2", rev))).To(BeFalse())

			reconcileOnce(r, "drift2")
			Expect(hasReadinessGate(getDep("drift2", rev))).To(BeTrue(),
				"a removed readiness gate must be restored")
		})

		It("does not issue an Update when the Deployment has not drifted", func() {
			r := readyReconciler()
			rev := promote(r, "drift3")

			before := getDep("drift3", rev).ResourceVersion
			reconcileOnce(r, "drift3")
			after := getDep("drift3", rev).ResourceVersion
			Expect(after).To(Equal(before),
				"a no-drift reconcile must not write the Deployment (resourceVersion unchanged)")
		})

		It("restores the Service selector after it is changed by hand", func() {
			r := readyReconciler()
			rev := promote(r, "drift4")
			Expect(getSvc("drift4").Spec.Selector[decisionmodelv1alpha1.LabelRevision]).To(Equal(rev))

			// Point the selector at a bogus revision.
			Expect(k8sClient.Update(ctx, func() *corev1.Service {
				svc := getSvc("drift4")
				svc.Spec.Selector[decisionmodelv1alpha1.LabelRevision] = "bogus"
				return svc
			}())).To(Succeed())
			Expect(getSvc("drift4").Spec.Selector[decisionmodelv1alpha1.LabelRevision]).To(Equal("bogus"))

			reconcileOnce(r, "drift4")
			Expect(getSvc("drift4").Spec.Selector[decisionmodelv1alpha1.LabelRevision]).To(Equal(rev),
				"a hand-edited Service selector must be restored to the stable revision")
		})

		It("does not issue an Update when the Service has not drifted", func() {
			r := readyReconciler()
			promote(r, "drift5")

			before := getSvc("drift5").ResourceVersion
			reconcileOnce(r, "drift5")
			after := getSvc("drift5").ResourceVersion
			Expect(after).To(Equal(before),
				"a no-drift reconcile must not write the Service (resourceVersion unchanged)")
		})
	})

	// namespace-scoped mode. A DecisionModel in a namespace outside the
	// reconciler's WatchNamespaces set must be ignored entirely — no PVC, no Job,
	// no status writes — as defence in depth behind the namespace-scoped cache.
	Context("namespace-scoped mode", func() {
		It("ignores a DecisionModel outside the watched namespaces", func() {
			eng := newFakeEngine()
			r := newReconciler(eng, &fakeProber{})
			// Watch only some other namespace, not the test namespace.
			r.WatchNamespaces = []string{"some-other-namespace"}

			createDM("nsmode1", nil)
			reconcileOnce(r, "nsmode1")

			// No store PVC and no prefetch Job were created for the ignored DM.
			pvc := &corev1.PersistentVolumeClaim{}
			err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "nsmode1-store"}, pvc)
			Expect(apierrors.IsNotFound(err)).To(BeTrue(), "PVC must not be created for an unwatched namespace")

			var jobs batchv1.JobList
			Expect(k8sClient.List(ctx, &jobs, client.InNamespace(namespace))).To(Succeed())
			Expect(jobs.Items).To(BeEmpty(), "no prefetch Job for an unwatched namespace")

			// The engine was never consulted and status stayed empty (no phase set).
			Expect(eng.resolveCount()).To(Equal(0), "engine must not be resolved for an unwatched DM")
			Expect(getDM("nsmode1").Status.Phase).To(BeEmpty())
		})

		It("reconciles a DecisionModel inside the watched namespaces", func() {
			eng := newFakeEngine()
			r := newReconciler(eng, &fakeProber{})
			// Watch the test namespace explicitly.
			r.WatchNamespaces = []string{namespace}

			createDM("nsmode2", nil)
			reconcileOnce(r, "nsmode2")

			// Normal Caching behaviour: PVC + prefetch Job exist.
			rev := revOf(getDM("nsmode2"))
			pvc := &corev1.PersistentVolumeClaim{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "nsmode2-store-" + rev}, pvc)).To(Succeed())
			job := &batchv1.Job{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: namespace, Name: "nsmode2-prefetch-" + rev,
			}, job)).To(Succeed())
			Expect(getDM("nsmode2").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseCaching))
		})
	})

	// blue-green on RWO/GPU. Each revision owns its store PVC
	// (<dm>-store-<rev>), and a recorded stable revision is rendered from its own
	// fields so a pending/failed candidate never rewrites the stable Deployment.
	Context("blue-green per-revision store", func() {
		mem := func(q string) corev1.ResourceRequirements {
			return corev1.ResourceRequirements{
				Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse(q)},
			}
		}

		readyReconciler := func() (*fakeEngine, *DecisionModelReconciler) {
			eng := newFakeEngine()
			r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{
				Name: model, Digest: digest, Device: "cpu", Precision: "F32",
			}})
			return eng, r
		}

		// promote drives a fresh DM to Ready and returns its revision hash.
		promote := func(r *DecisionModelReconciler, name string, mutate func(*decisionmodelv1alpha1.DecisionModel)) string {
			createDM(name, mutate)
			reconcileOnce(r, name)
			rev := revOf(getDM(name))
			markJob(name, rev, batchv1.JobComplete)
			reconcileOnce(r, name)
			createReadyPod(name, rev, name+"-pod-"+rev)
			reconcileOnce(r, name) // probe -> gate True
			reconcileOnce(r, name) // promote
			Expect(getDM(name).Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseReady))
			Expect(getDM(name).Status.StableRevision.Hash).To(Equal(rev))
			return rev
		}

		pvcExists := func(name, rev string) bool {
			pvc := &corev1.PersistentVolumeClaim{}
			err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-store-" + rev}, pvc)
			if err == nil {
				return true
			}
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
			return false
		}

		// pvcDeleted reports whether a per-revision PVC has been deleted. In
		// envtest the pvc-protection finalizer (no PV controller to clear it) keeps
		// a deleted PVC in Terminating with a DeletionTimestamp rather than
		// removing it, so a requested deletion counts as deleted.
		pvcDeleted := func(name, rev string) bool {
			pvc := &corev1.PersistentVolumeClaim{}
			err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-store-" + rev}, pvc)
			if apierrors.IsNotFound(err) {
				return true
			}
			Expect(err).NotTo(HaveOccurred())
			return pvc.DeletionTimestamp != nil
		}

		getDep := func(name, rev string) *appsv1.Deployment {
			dep := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-" + rev}, dep)).To(Succeed())
			return dep
		}

		claimOf := func(dep *appsv1.Deployment) string {
			for _, v := range dep.Spec.Template.Spec.Volumes {
				if v.PersistentVolumeClaim != nil {
					return v.PersistentVolumeClaim.ClaimName
				}
			}
			return ""
		}

		// Bug 1: a rollout creates the new revision's own PVC; the stable keeps its
		// own PVC; after promotion + grace the old revision's PVC is deleted.
		It("gives each revision its own store PVC and GCs the old one after promotion", func() {
			_, r := readyReconciler()
			rev1 := promote(r, "bg1", nil)
			Expect(pvcExists("bg1", rev1)).To(BeTrue())
			Expect(claimOf(getDep("bg1", rev1))).To(Equal("bg1-store-" + rev1))

			// Change resources -> new revision (same digest), the Bug 1 repro.
			Expect(updateDM(ctx, namespace, "bg1", func(dm *decisionmodelv1alpha1.DecisionModel) {
				dm.Spec.Resources = mem("5Gi")
			})).To(Succeed())
			reconcileOnce(r, "bg1") // resolve + cache new rev
			rev2 := revOf(getDM("bg1"))
			Expect(rev2).NotTo(Equal(rev1))

			// New revision has its own PVC; stable's PVC still present (blue-green).
			Expect(pvcExists("bg1", rev2)).To(BeTrue())
			Expect(pvcExists("bg1", rev1)).To(BeTrue())

			// Promote rev2.
			markJob("bg1", rev2, batchv1.JobComplete)
			reconcileOnce(r, "bg1")
			Expect(claimOf(getDep("bg1", rev2))).To(Equal("bg1-store-" + rev2))
			createReadyPod("bg1", rev2, "bg1-pod-"+rev2)
			reconcileOnce(r, "bg1") // probe
			reconcileOnce(r, "bg1") // promote (old revision deferred by grace)
			Expect(getDM("bg1").Status.StableRevision.Hash).To(Equal(rev2))

			// Within grace the old PVC survives; after grace it is GC'd.
			Expect(pvcDeleted("bg1", rev1)).To(BeFalse(), "old PVC kept within promote grace")
			r.Now = func() time.Time { return time.Now().Add(2 * promoteGrace) }
			reconcileOnce(r, "bg1")
			Expect(pvcDeleted("bg1", rev1)).To(BeTrue(), "old revision PVC GC'd after grace")
			Expect(pvcDeleted("bg1", rev2)).To(BeFalse(), "new stable PVC kept")
		})

		// A failed candidate's PVC is deleted with its workloads.
		It("deletes a failed revision's store PVC with its workloads", func() {
			_, r := readyReconciler()
			rev1 := promote(r, "bg2", nil)

			// New revision whose prefetch fails.
			Expect(updateDM(ctx, namespace, "bg2", func(dm *decisionmodelv1alpha1.DecisionModel) {
				dm.Spec.Resources = mem("7Gi")
			})).To(Succeed())
			reconcileOnce(r, "bg2")
			rev2 := revOf(getDM("bg2"))
			Expect(pvcExists("bg2", rev2)).To(BeTrue())

			markJob("bg2", rev2, batchv1.JobFailed)
			reconcileOnce(r, "bg2") // rollback
			Expect(getDM("bg2").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseRolledBack))

			// Failed revision's PVC gone; stable's PVC kept.
			Expect(pvcDeleted("bg2", rev2)).To(BeTrue(), "failed revision PVC deleted with its workloads")
			Expect(pvcDeleted("bg2", rev1)).To(BeFalse(), "stable PVC kept")
		})

		// Bug 2: a failed candidate must not rewrite the stable Deployment with the
		// candidate's spec; and the stable Pod template still hashes to the stable
		// revision while the candidate is pending/failed.
		It("keeps the stable Deployment rendered from the stable revision when a candidate fails", func() {
			_, r := readyReconciler()
			rev1 := promote(r, "bg3", nil) // default resources (empty)
			stableDep := getDep("bg3", rev1)
			stableHash := stableDep.Annotations[specHashAnnotation]
			stableRV := stableDep.ResourceVersion

			// New candidate with different resources.
			Expect(updateDM(ctx, namespace, "bg3", func(dm *decisionmodelv1alpha1.DecisionModel) {
				dm.Spec.Resources = mem("5Gi")
			})).To(Succeed())
			reconcileOnce(r, "bg3")
			rev2 := revOf(getDM("bg3"))
			markJob("bg3", rev2, batchv1.JobFailed)
			reconcileOnce(r, "bg3") // rollback
			Expect(getDM("bg3").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseRolledBack))

			// Stable Deployment unchanged: same spec-hash annotation + resourceVersion,
			// and its container memory limit is still the stable's (not 5Gi).
			stableDep2 := getDep("bg3", rev1)
			Expect(stableDep2.Annotations[specHashAnnotation]).To(Equal(stableHash),
				"failed candidate must not rewrite the stable Deployment spec")
			Expect(stableDep2.ResourceVersion).To(Equal(stableRV),
				"stable Deployment resourceVersion must be unchanged")
			stableMem := stableDep2.Spec.Template.Spec.Containers[0].Resources.Limits.Memory()
			Expect(stableMem.IsZero()).To(BeTrue(), "stable kept its own (empty) resources, not the candidate's 5Gi")

			// Invariant: the rendered stable Pod template still hashes to rev1.
			Expect(revisionHashFromStatus(getDM("bg3").Status.StableRevision)).To(Equal(rev1))
		})

		// Legacy migration: a stable whose live Deployment mounts the shared
		// <dm>-store keeps it until the next promotion, after which <dm>-store is
		// removed and the new revision uses its own PVC.
		It("migrates a legacy shared store to per-revision on the next promotion", func() {
			eng, r := readyReconciler()
			rev1 := promote(r, "bg4", nil)

			// Simulate a legacy stable: create the shared <dm>-store PVC and rewrite
			// the stable Deployment's store volume to mount it (as a legacy shared store would).
			// Even the original implementation set a controller OwnerReference
			// on <dm>-store, so the fixture does too —
			// the ownership guards only ever touch objects this DM owns.
			legacy := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace, Name: "bg4-store",
					Labels: map[string]string{decisionmodelv1alpha1.LabelName: "bg4"},
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
					},
				},
			}
			Expect(controllerutil.SetControllerReference(getDM("bg4"), legacy, r.Scheme)).To(Succeed())
			Expect(k8sClient.Create(ctx, legacy)).To(Succeed())
			dep := getDep("bg4", rev1)
			for i := range dep.Spec.Template.Spec.Volumes {
				if dep.Spec.Template.Spec.Volumes[i].PersistentVolumeClaim != nil {
					dep.Spec.Template.Spec.Volumes[i].PersistentVolumeClaim.ClaimName = "bg4-store"
				}
			}
			Expect(k8sClient.Update(ctx, dep)).To(Succeed())

			// A stable reconcile keeps the legacy claim (no rewrite to per-revision).
			reconcileOnce(r, "bg4")
			Expect(claimOf(getDep("bg4", rev1))).To(Equal("bg4-store"),
				"legacy stable keeps the shared store until the next promotion")
			legacyPVC := &corev1.PersistentVolumeClaim{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "bg4-store"}, legacyPVC)).To(Succeed())

			// Roll out a new revision and promote it.
			eng.digest = "a0a0000000000000000000000000000000000000000000000000000000000000"
			r.Prober = &fakeProber{loaded: engine.Loaded{Name: "kev:en", Digest: eng.digest, Device: "cpu", Precision: "F32"}}
			Expect(updateDM(ctx, namespace, "bg4", func(dm *decisionmodelv1alpha1.DecisionModel) {
				dm.Spec.Model = "kev:en"
			})).To(Succeed())
			reconcileOnce(r, "bg4")
			rev2 := RevisionHash(getDM("bg4").Spec, eng.digest, fakeImage)
			Expect(rev2).NotTo(Equal(rev1))
			Expect(pvcExists("bg4", rev2)).To(BeTrue())
			markJob("bg4", rev2, batchv1.JobComplete)
			reconcileOnce(r, "bg4")
			createReadyPod("bg4", rev2, "bg4-pod-"+rev2)
			reconcileOnce(r, "bg4") // probe
			reconcileOnce(r, "bg4") // promote
			Expect(getDM("bg4").Status.StableRevision.Hash).To(Equal(rev2))
			Expect(claimOf(getDep("bg4", rev2))).To(Equal("bg4-store-" + rev2))

			// After grace the old revision workloads are GC'd; the legacy shared
			// store is no longer referenced and is removed.
			r.Now = func() time.Time { return time.Now().Add(2 * promoteGrace) }
			reconcileOnce(r, "bg4") // GCs the old rev1 Deployment
			reconcileOnce(r, "bg4") // legacy store now unreferenced -> removed
			gone := &corev1.PersistentVolumeClaim{}
			err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "bg4-store"}, gone)
			// envtest keeps a deleted PVC in Terminating (pvc-protection finalizer),
			// so a NotFound or a set DeletionTimestamp both mean "removed".
			Expect(apierrors.IsNotFound(err) || gone.DeletionTimestamp != nil).To(BeTrue(),
				"legacy shared store removed once unreferenced")
		})

		// No-op reconcile: a steady-state stable issues no PVC/Deployment write.
		It("does not write PVC or Deployment on a no-drift reconcile", func() {
			_, r := readyReconciler()
			rev := promote(r, "bg5", nil)
			depRV := getDep("bg5", rev).ResourceVersion
			pvc := &corev1.PersistentVolumeClaim{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "bg5-store-" + rev}, pvc)).To(Succeed())
			pvcRV := pvc.ResourceVersion

			reconcileOnce(r, "bg5")

			Expect(getDep("bg5", rev).ResourceVersion).To(Equal(depRV), "no Deployment write on no-op reconcile")
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "bg5-store-" + rev}, pvc)).To(Succeed())
			Expect(pvc.ResourceVersion).To(Equal(pvcRV), "no PVC write on no-op reconcile")
		})
	})
})
