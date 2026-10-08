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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// Candidate PDB before the traffic switch; missing Secret as a condition; a
// stray retry token is consumed.
var _ = Describe("PDB-before-switch, missing Secret, retry token", func() {
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
	newR := func(cl client.Client) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client: cl, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:  map[string]engine.Engine{"ollaya": newFakeEngine()},
			Prober:   &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}},
			Recorder: events.NewFakeRecorder(256),
		}
	}
	rec := func(r *DecisionModelReconciler, name string) {
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
	}
	markJob := func(name, rev string) {
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
	readyPods := func(name, rev string, ips ...string) {
		for i, ip := range ips {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: fmt.Sprintf("%s-pod-%s-%d", name, rev, i),
					Labels: map[string]string{decisionmodelv1alpha1.LabelName: name, decisionmodelv1alpha1.LabelRevision: rev}},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
			}
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())
			pod.Status.PodIP = ip
			pod.Status.Conditions = []corev1.PodCondition{
				{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
			}
			Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
		}
	}
	drainHas := func(r *DecisionModelReconciler, substr string) bool {
		fr := r.Recorder.(*events.FakeRecorder)
		for {
			select {
			case e := <-fr.Events:
				if strings.Contains(e, substr) {
					return true
				}
			default:
				return false
			}
		}
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		namespace = fmt.Sprintf("srp-%d", counter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	// With replicas > 1 the candidate's PodDisruptionBudget must exist before the
	// Service is switched to it, so there is never a window with live traffic and
	// no disruption protection. Intercept the Service write and assert the PDB is
	// already present.
	It("creates the candidate PDB before switching the Service", func() {
		pdbSeenAtSwitch := false
		wc, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		checkPDB := func(ctx context.Context, cl client.WithWatch, obj client.Object) {
			svc, ok := obj.(*corev1.Service)
			if !ok {
				return
			}
			rv := svc.Spec.Selector[decisionmodelv1alpha1.LabelRevision]
			if rv == "" {
				return
			}
			pdb := &policyv1.PodDisruptionBudget{}
			if err := cl.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "pdbsw-" + rv}, pdb); err == nil {
				pdbSeenAtSwitch = true
			}
		}
		c := interceptor.NewClient(wc, interceptor.Funcs{
			Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				checkPDB(ctx, cl, obj)
				return cl.Update(ctx, obj, opts...)
			},
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				checkPDB(ctx, cl, obj)
				return cl.Create(ctx, obj, opts...)
			},
		})
		r := newR(c)
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "pdbsw"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(2),
			},
		})).To(Succeed())
		rec(r, "pdbsw")
		rev := RevisionHash(getDM("pdbsw").Spec, defaultDigest, fakeImage)
		markJob("pdbsw", rev)
		rec(r, "pdbsw")
		readyPods("pdbsw", rev, "10.0.0.40", "10.0.0.41")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			rec(r, "pdbsw")
			return getDM("pdbsw").Status.Phase
		}, "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))

		Expect(pdbSeenAtSwitch).To(BeTrue(), "the candidate PDB existed before the Service was switched to it")
		pdb := &policyv1.PodDisruptionBudget{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "pdbsw-" + rev}, pdb)).To(Succeed())
	})

	// A missing API-key Secret surfaces as Degraded=APIKeyInvalid (naming the
	// Secret), with no stable; it is a held condition, not a reconcile error.
	It("surfaces a missing API-key Secret as APIKeyInvalid, not a reconcile error", func() {
		r := newR(k8sClient)
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "nokey"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
				Auth: &decisionmodelv1alpha1.AuthSpec{APIKeySecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "absent-key"}, Key: "token",
				}},
			},
		})).To(Succeed())
		res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "nokey"}})
		Expect(err).NotTo(HaveOccurred(), "a missing Secret is a condition, not a reconcile error")
		Expect(res.RequeueAfter).To(BeNumerically(">", 0), "held with a requeue")
		dm := getDM("nokey")
		Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseDegraded))
		ready := meta_Find(dm, decisionmodelv1alpha1.ConditionReady)
		Expect(ready).NotTo(BeNil())
		Expect(ready.Reason).To(Equal(reasonAPIKeyInvalid))
		Expect(ready.Message).To(ContainSubstring("absent-key"), "names the Secret, not its content")
	})

	// A decisionmodel.io/retry token set when there is nothing to retry must be
	// consumed on first observation (so it cannot arm a later failure) and emit a
	// RetryNoop Event once.
	It("consumes a stray retry token with a RetryNoop Event", func() {
		r := newR(k8sClient)
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "retry0",
				Annotations: map[string]string{decisionmodelv1alpha1.AnnotationRetry: "tok-1"}},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
		// First reconcile: nothing has failed, so the token is a no-op but consumed.
		rec(r, "retry0")
		Expect(getDM("retry0").Status.LastRetryToken).To(Equal("tok-1"), "stray token consumed")
		Expect(drainHas(r, eventRetryNoop)).To(BeTrue(), "RetryNoop Event emitted once")

		// The same token on later reconciles does nothing (already consumed).
		for i := 0; i < 3; i++ {
			rec(r, "retry0")
		}
		Expect(drainHas(r, eventRetryNoop)).To(BeFalse(), "no repeat RetryNoop for the same token")
		Expect(getDM("retry0").Status.LastRetryToken).To(Equal("tok-1"))
	})
})
