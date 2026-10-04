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
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// --- §3 IPv6 URL building (pure) -----------------------------------------

var _ = Describe("podBaseURL IPv6", func() {
	It("brackets an IPv6 literal and leaves IPv4 unbracketed", func() {
		Expect(podBaseURL("fd00::123", 11435)).To(Equal("http://[fd00::123]:11435"))
		Expect(podBaseURL("10.0.0.5", 11435)).To(Equal("http://10.0.0.5:11435"))
	})
})

// --- §4 ownership (defence in depth) -------------------------------------

var _ = Describe("ownership checks", func() {
	var (
		ctx       context.Context
		namespace string
		nsCounter int
	)
	int32Ptr := func(v int32) *int32 { return &v }

	newReconciler := func(eng engine.Engine, prober Prober) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client:    k8sClient,
			APIReader: k8sClient,
			Scheme:    k8sClient.Scheme(),
			Engines:   map[string]engine.Engine{"ollaya": eng},
			Prober:    prober,
			Recorder:  events.NewFakeRecorder(64),
		}
	}

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = "own-test-" + itoa(nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
		withPodOwnership()
	})

	// mkDM creates a DecisionModel and returns it (with a populated UID).
	mkDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
		return dm
	}

	// ownedChain builds a real Deployment(owned by dm) -> ReplicaSet(owned by the
	// Deployment) -> Pod(owned by the ReplicaSet) for revision rev.
	ownedChain := func(r *DecisionModelReconciler, dm *decisionmodelv1alpha1.DecisionModel, rev, podName string) {
		labels := map[string]string{
			decisionmodelv1alpha1.LabelName:     dm.Name,
			decisionmodelv1alpha1.LabelRevision: rev,
		}
		dep := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: revisionName(dm, rev), Labels: labels},
			Spec: appsv1.DeploymentSpec{
				Replicas: int32Ptr(1),
				Selector: &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
				},
			},
		}
		Expect(controllerutil.SetControllerReference(dm, dep, r.Scheme)).To(Succeed())
		Expect(k8sClient.Create(ctx, dep)).To(Succeed())

		rs := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: revisionName(dm, rev) + "-rs", Labels: labels},
			Spec: appsv1.ReplicaSetSpec{
				Replicas: int32Ptr(1),
				Selector: &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
				},
			},
		}
		Expect(controllerutil.SetControllerReference(dep, rs, r.Scheme)).To(Succeed())
		Expect(k8sClient.Create(ctx, rs)).To(Succeed())

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: podName, Labels: labels},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
		}
		Expect(controllerutil.SetControllerReference(rs, pod, r.Scheme)).To(Succeed())
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.PodIP = "10.0.0.40"
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}

	// foreignPod creates a bare Pod with matching labels but no owner chain to
	// this DM — the kind of Pod an unrelated workload in the namespace could set.
	foreignPod := func(dm *decisionmodelv1alpha1.DecisionModel, rev, podName string) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: podName,
				Labels: map[string]string{
					decisionmodelv1alpha1.LabelName:     dm.Name,
					decisionmodelv1alpha1.LabelRevision: rev,
				},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "x", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.PodIP = "10.0.0.99"
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}

	It("probes a Pod whose controller chain leads to this DM, not a foreign Pod with matching labels", func() {
		eng := newFakeEngine()
		prober := &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		r := newReconciler(eng, prober)
		dm := mkDM("own1")
		rev := RevisionHash(dm.Spec, defaultDigest, fakeImage)

		foreignPod(dm, rev, "foreign")
		pods, err := r.revisionPods(ctx, dm, rev)
		Expect(err).NotTo(HaveOccurred())
		Expect(pods).To(BeEmpty(), "foreign Pod (labels only) must not be selected")

		ownedChain(r, dm, rev, "owned")
		pods, err = r.revisionPods(ctx, dm, rev)
		Expect(err).NotTo(HaveOccurred())
		Expect(pods).To(HaveLen(1))
		Expect(pods[0].Name).To(Equal("owned"), "only the owned Pod is selected")
	})

	// a ReplicaSet lookup failure must surface as errPodListFailed (abort
	// / workqueue backoff), never be mistaken for "0 owned Pods".
	It("returns errPodListFailed when the ReplicaSet lookup fails (not 0 Pods)", func() {
		eng := newFakeEngine()
		dm := mkDM("own-rserr")
		rev := RevisionHash(dm.Spec, defaultDigest, fakeImage)

		var fail atomic.Bool
		wc, werr := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(werr).NotTo(HaveOccurred())
		c := interceptor.NewClient(wc, interceptor.Funcs{
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*appsv1.ReplicaSet); ok && fail.Load() {
					return fmt.Errorf("boom: replicaset get unavailable")
				}
				return cl.Get(ctx, key, obj, opts...)
			},
		})
		r := &DecisionModelReconciler{
			Client: c, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:  map[string]engine.Engine{"ollaya": eng},
			Prober:   &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}},
			Recorder: events.NewFakeRecorder(16),
		}
		ownedChain(r, dm, rev, "owned")

		fail.Store(true)
		_, err := r.revisionPods(ctx, dm, rev)
		Expect(err).To(MatchError(errPodListFailed), "RS lookup failure must abort, not look like 0 Pods")
	})

	It("GC never deletes a foreign Deployment/PVC with matching labels", func() {
		eng := newFakeEngine()
		r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}})
		dm := mkDM("own2")
		staleRev := "deadbeefdeadbeef"
		labels := map[string]string{
			decisionmodelv1alpha1.LabelName:     dm.Name,
			decisionmodelv1alpha1.LabelRevision: staleRev,
		}
		// A foreign Deployment with this DM's labels and a stale revision, but no
		// controller OwnerReference to this DM.
		foreignDep := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: dm.Name + "-" + staleRev, Labels: labels},
			Spec: appsv1.DeploymentSpec{
				Replicas: int32Ptr(1),
				Selector: &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "x", Image: fakeImage}}},
				},
			},
		}
		Expect(k8sClient.Create(ctx, foreignDep)).To(Succeed())
		foreignPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: dm.Name + "-store-" + staleRev, Labels: labels},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
			},
		}
		Expect(k8sClient.Create(ctx, foreignPVC)).To(Succeed())

		Expect(r.gcRevisions(ctx, dm)).To(Succeed())

		// Both foreign objects survive: GC deletes only objects this DM controls.
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: foreignDep.Name}, &appsv1.Deployment{})).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: foreignPVC.Name}, &corev1.PersistentVolumeClaim{})).To(Succeed())
	})
})

// --- §1 baseline fail-closed ---------------------------------------------

var _ = Describe("baseline fail-closed", func() {
	const model = "laya:en"
	var (
		ctx       context.Context
		namespace string
		nsCounter int
	)
	int32Ptr := func(v int32) *int32 { return &v }

	newReconciler := func(eng engine.Engine, prober Prober) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client:    k8sClient,
			APIReader: k8sClient,
			Scheme:    k8sClient.Scheme(),
			Engines:   map[string]engine.Engine{"ollaya": eng},
			Prober:    prober,
			Recorder:  events.NewFakeRecorder(64),
		}
	}

	reconcileOnce := func(r *DecisionModelReconciler, name string) {
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
	}
	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}
	createReadyPod := func(dmName, rev, podName string) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: podName,
				Labels: map[string]string{decisionmodelv1alpha1.LabelName: dmName, decisionmodelv1alpha1.LabelRevision: rev},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.PodIP = "10.0.0.21"
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}
	markJobComplete := func(dmName, rev string) {
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: dmName + "-prefetch-" + rev}, job)).To(Succeed())
		now := metav1.Now()
		job.Status.StartTime = &now
		job.Status.CompletionTime = &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = "fc-test-" + itoa(nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	It("holds Evaluating=BaselineUnavailable then rolls back when a relative gate has no reachable baseline", func() {
		clock := newSafeClock()
		eng := &deciderFakeEngine{fakeEngine: newFakeEngine(), choice: "billing"}
		r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{Name: model, Digest: defaultDigest, Device: "cpu"}})
		r.Now = clock.now

		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "golden"},
			Data:       map[string]string{"cases.jsonl": `{"state":{},"questions":{"q1":{"type":"choice"}},"expected":{"q1":"billing"}}` + "\n"},
		}
		Expect(k8sClient.Create(ctx, cm)).To(Succeed())

		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "fc1"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model, Device: "cpu", Replicas: int32Ptr(1),
				Rollout: &decisionmodelv1alpha1.RolloutSpec{Evaluation: &decisionmodelv1alpha1.EvaluationSpec{
					DatasetRef:      decisionmodelv1alpha1.DatasetRef{ConfigMapRef: &decisionmodelv1alpha1.DatasetKeyRef{Name: "golden", Key: "cases.jsonl"}},
					MinAccuracy:     "0.50",
					MaxAccuracyDrop: "0.02", // relative gate -> baseline required
				}},
			},
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())

		// A stable revision exists in status, but there is NO reachable stable Pod
		// (none created), so the baseline cannot be computed.
		Expect(k8sClient.Status().Update(ctx, func() *decisionmodelv1alpha1.DecisionModel {
			d := getDM("fc1")
			d.Status.StableRevision = &decisionmodelv1alpha1.RevisionStatus{
				Hash: "0000stable0000", Engine: "ollaya", Model: model, Digest: defaultDigest, Device: "cpu",
			}
			return d
		}())).To(Succeed())

		// Drive the candidate to Evaluating with its own ready Pod.
		reconcileOnce(r, "fc1")
		rev := RevisionHash(getDM("fc1").Spec, defaultDigest, fakeImage)
		markJobComplete("fc1", rev)
		reconcileOnce(r, "fc1")
		createReadyPod("fc1", rev, "fc1-cand")

		// The candidate eval completes (accuracy passes minAccuracy), but the
		// baseline cannot be computed: hold in Evaluating / BaselineUnavailable.
		Eventually(func() string {
			reconcileOnce(r, "fc1")
			c := meta_Find(getDM("fc1"), decisionmodelv1alpha1.ConditionEvaluated)
			if c == nil {
				return ""
			}
			return c.Reason
		}, "5s", "50ms").Should(Equal(reasonBaselineUnavailable))
		Expect(getDM("fc1").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseEvaluating))
		Expect(getDM("fc1").Status.StableRevision).NotTo(BeNil(), "did not promote without a baseline")

		// Past the evaluation timeout -> normal rollback path (stable kept).
		clock.add(evaluatingTimeout(getDM("fc1")) + time.Minute)
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			reconcileOnce(r, "fc1")
			return getDM("fc1").Status.Phase
		}, "5s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseRolledBack))
	})
})
