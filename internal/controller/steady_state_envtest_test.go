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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// Once a DecisionModel has converged to a steady state, repeated reconciles must
// be no-ops: no object writes and a stable status resourceVersion. Level-triggered
// controllers that write on every reconcile cause hot loops and event storms.
var _ = Describe("steady state is write-free", func() {
	var (
		ctx       context.Context
		namespace string
		counter   int
	)
	int32Ptr := func(v int32) *int32 { return &v }
	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}
	markJob := func(cl client.Client, name, rev string) {
		job := &batchv1.Job{}
		Expect(cl.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName(name, rev)}, job)).To(Succeed())
		now := metav1.Now()
		job.Status.StartTime, job.Status.CompletionTime = &now, &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
		}
		Expect(cl.Status().Update(ctx, job)).To(Succeed())
	}
	gatedPod := func(cl client.Client, name, rev, ip string) {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: name + "-pod-" + rev,
				Labels: map[string]string{decisionmodelv1alpha1.LabelName: name, decisionmodelv1alpha1.LabelRevision: rev},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
		}
		Expect(cl.Create(ctx, p)).To(Succeed())
		p.Status.PodIP = ip
		p.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(cl.Status().Update(ctx, p)).To(Succeed())
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		namespace = fmt.Sprintf("steady-%d", counter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	It("writes nothing and keeps the status resourceVersion stable over 30 reconciles once Ready", func() {
		pr := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		// Count object mutations (Create/Update/Patch, incl. status) through an
		// interceptor so a steady-state write is caught no matter the object.
		var writes atomic.Int32
		wc, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		count := func() { writes.Add(1) }
		c := interceptor.NewClient(wc, interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				count()
				return cl.Create(ctx, obj, opts...)
			},
			Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				count()
				return cl.Update(ctx, obj, opts...)
			},
			Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				count()
				return cl.Patch(ctx, obj, patch, opts...)
			},
			SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				count()
				return cl.SubResource(sub).Update(ctx, obj, opts...)
			},
			SubResourcePatch: func(ctx context.Context, cl client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				count()
				return cl.Status().Patch(ctx, obj, patch, opts...)
			},
		})
		r := &DecisionModelReconciler{
			Client: c, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines: map[string]engine.Engine{"ollaya": newFakeEngine()}, Prober: pr,
			Recorder: events.NewFakeRecorder(256),
		}
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "s"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
		rec := func() {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "s"}})
		}
		rec()
		rev := RevisionHash(getDM("s").Spec, defaultDigest, fakeImage)
		pr.set(rev, engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"})
		markJob(c, "s", rev)
		rec()
		gatedPod(c, "s", rev, "10.0.30.1")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			rec()
			return getDM("s").Status.Phase
		}, "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))

		// Converged. Record the status RV, then reconcile 30 times and assert no
		// object write happened and the status RV did not move.
		rvBefore := getDM("s").ResourceVersion
		writes.Store(0)
		for i := 0; i < 30; i++ {
			rec()
		}
		Expect(writes.Load()).To(BeEquivalentTo(0), "a converged DecisionModel must not write on reconcile")
		Expect(getDM("s").ResourceVersion).To(Equal(rvBefore), "status resourceVersion is stable once converged")
	})
})
