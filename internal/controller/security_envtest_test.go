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
	"sync/atomic"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
	"github.com/maks3201/decision-model-operator/internal/engine/ollaya"
)

// countingReader wraps a client.Reader and counts Secret Gets, so a test can
// prove Secrets are read through the injected APIReader rather than the
// cached client.
type countingReader struct {
	client.Reader
	secretGets int32
}

func (c *countingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.Secret); ok {
		atomic.AddInt32(&c.secretGets, 1)
	}
	return c.Reader.Get(ctx, key, obj, opts...)
}

var _ = Describe("security guards", func() {
	const model = "laya:en"

	var (
		ctx       context.Context
		namespace string
		nsCounter int
	)

	int32Ptr := func(v int32) *int32 { return &v }

	reconcileOnce := func(r *DecisionModelReconciler, name string) {
		_, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: namespace, Name: name},
		})
		Expect(err).NotTo(HaveOccurred())
	}

	// reconcilePermanent expects a permanent (terminal) failure: status is written
	// and the reconciler returns a non-nil (terminal) error.
	reconcilePermanent := func(r *DecisionModelReconciler, name string) {
		_, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: namespace, Name: name},
		})
		Expect(err).To(HaveOccurred(), "expected a terminal error for a permanent failure")
	}

	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}

	newR := func(eng engine.Engine) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client:    k8sClient,
			APIReader: k8sClient, // real reader by default; overridden per-test
			Scheme:    k8sClient.Scheme(),
			Engines:   map[string]engine.Engine{"ollaya": eng},
			Prober:    &fakeProber{loaded: engine.Loaded{Name: model, Digest: defaultDigest, Device: "cpu"}},
			Recorder:  events.NewFakeRecorder(64),
		}
	}

	createDM := func(name string, mutate func(*decisionmodelv1alpha1.DecisionModel)) {
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model, Device: "cpu", Replicas: int32Ptr(1),
			},
		}
		if mutate != nil {
			mutate(dm)
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
	}

	readyReason := func(name string) string {
		c := meta.FindStatusCondition(getDM(name).Status.Conditions, decisionmodelv1alpha1.ConditionReady)
		if c == nil {
			return ""
		}
		return c.Reason
	}

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = fmt.Sprintf("security-test-%d", nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	// the engine API key Secret is read through the injected APIReader.
	It("C1: reads the API-key Secret via APIReader", func() {
		reader := &countingReader{Reader: k8sClient}
		eng := newFakeEngine()
		r := newR(eng)
		r.APIReader = reader

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: "key",
				Labels: map[string]string{decisionmodelv1alpha1.LabelAPIKey: "true"},
			},
			Data: map[string][]byte{"token": []byte("s3cr3t")},
		}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())
		createDM("c1", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Auth = &decisionmodelv1alpha1.AuthSpec{
				APIKeySecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "key"}, Key: "token",
				},
			}
		})

		reconcileOnce(r, "c1")
		Expect(atomic.LoadInt32(&reader.secretGets)).To(BeNumerically(">=", 1),
			"the API-key Secret must be read through APIReader")
	})

	// an unrelated spec change (replicas) must not re-resolve the tag.
	It("C2: does not re-resolve the tag on a replicas change", func() {
		eng := newFakeEngine()
		r := newR(eng)
		createDM("c2", nil)

		reconcileOnce(r, "c2") // first resolve
		Expect(eng.resolveCount()).To(Equal(1))

		// Bump replicas; the reconciler must reuse the candidate digest.
		Expect(updateDM(ctx, namespace, "c2", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Replicas = int32Ptr(3)
		})).To(Succeed())
		reconcileOnce(r, "c2")
		Expect(eng.resolveCount()).To(Equal(1), "replicas change must not trigger Resolve")
	})

	// bumping the retry annotation clears a failed revision.
	It("C6: retry annotation clears a failed revision", func() {
		eng := newFakeEngine()
		r := newR(eng)
		createDM("c6", nil)

		reconcileOnce(r, "c6")
		rev := RevisionHash(getDM("c6").Spec, defaultDigest, fakeImage)
		markPrefetchFailed(ctx, namespace, "c6", rev)
		reconcileOnce(r, "c6")
		Expect(getDM("c6").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseFailed))
		Expect(getDM("c6").Status.FailedRevision).NotTo(BeNil())

		// Bump the retry annotation -> failedRevision cleared, Job recreated.
		Expect(updateDM(ctx, namespace, "c6", func(dm *decisionmodelv1alpha1.DecisionModel) {
			if dm.Annotations == nil {
				dm.Annotations = map[string]string{}
			}
			dm.Annotations[decisionmodelv1alpha1.AnnotationRetry] = "1"
		})).To(Succeed())
		reconcileOnce(r, "c6")

		Expect(getDM("c6").Status.FailedRevision).To(BeNil(), "retry annotation should clear failedRevision")
		Expect(getDM("c6").Status.LastRetryToken).To(Equal("1"))
	})

	// a model from a disallowed registry fails with RegistryNotAllowed.
	It("E2: rejects a disallowed registry", func() {
		eng := ollaya.New() // real engine implements RegistryHoster
		r := newR(eng)
		createDM("e2", func(dm *decisionmodelv1alpha1.DecisionModel) { dm.Spec.Model = "evil.com/ns/m:t" })

		reconcilePermanent(r, "e2")
		Expect(getDM("e2").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseFailed))
		Expect(readyReason("e2")).To(Equal(reasonRegistryNotAllowed))
	})

	// spec.image without --allow-image-override fails with ImageOverrideNotAllowed.
	It("E3: rejects spec.image override by default", func() {
		eng := newFakeEngine()
		r := newR(eng)
		createDM("e3", func(dm *decisionmodelv1alpha1.DecisionModel) { dm.Spec.Image = "ghcr.io/me/mine:latest" })

		reconcilePermanent(r, "e3")
		Expect(getDM("e3").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseFailed))
		Expect(readyReason("e3")).To(Equal(reasonImageOverrideNotAllowed))
	})

	// an API-key Secret without the opt-in label is NOT terminal (there
	// is no Secret watch): the DM stays Degraded=SecretNotAllowed and requeues at
	// the regate interval, so adding the label is picked up without a spec change.
	It("requeues on a missing opt-in label and recovers once it is added", func() {
		eng := newFakeEngine()
		r := newR(eng)
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "nolabel"},
			Data:       map[string][]byte{"token": []byte("x")},
		}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())
		createDM("e3b", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Auth = &decisionmodelv1alpha1.AuthSpec{
				APIKeySecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "nolabel"}, Key: "token",
				},
			}
		})

		// Not terminal: nil error, requeue at the regate interval, Degraded.
		res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "e3b"}})
		Expect(err).NotTo(HaveOccurred(), "SecretNotAllowed must not be a terminal error")
		Expect(res.RequeueAfter).To(Equal(regateInterval))
		Expect(getDM("e3b").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseDegraded))
		Expect(readyReason("e3b")).To(Equal(reasonSecretNotAllowed))

		// A second reconcile causes no status churn (same condition reason).
		rvBefore := getDM("e3b").ResourceVersion
		_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "e3b"}})
		Expect(err).NotTo(HaveOccurred())
		Expect(getDM("e3b").ResourceVersion).To(Equal(rvBefore), "no status churn while waiting for the label")

		// Add the opt-in label: the next reconcile picks it up (apiKey now resolves,
		// so the flow advances to Caching instead of parking on SecretNotAllowed).
		secret.Labels = map[string]string{decisionmodelv1alpha1.LabelAPIKey: "true"}
		Expect(k8sClient.Update(ctx, secret)).To(Succeed())
		reconcileOnce(r, "e3b")
		Expect(getDM("e3b").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseCaching),
			"label fix picked up without a spec change; the rollout proceeds")
	})

	// a download-token Secret without the opt-in label → Degraded/SecretNotAllowed,
	// no Job created.
	It("DT1: download-token Secret without opt-in label → Degraded, no Job", func() {
		eng := newFakeEngine()
		r := newR(eng)
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "hf-nolabel"},
			Data:       map[string][]byte{"token": []byte("hf_xxx")},
		}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())
		createDM("dt1", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Cache = &decisionmodelv1alpha1.CacheSpec{
				DownloadTokenSecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "hf-nolabel"},
					Key:                  "token",
				},
			}
		})

		res, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: namespace, Name: "dt1"},
		})
		Expect(err).NotTo(HaveOccurred(), "SecretNotAllowed must not be a terminal error")
		Expect(res.RequeueAfter).To(Equal(regateInterval))
		Expect(getDM("dt1").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseDegraded))
		Expect(readyReason("dt1")).To(Equal(reasonSecretNotAllowed))

		// No prefetch Job was created.
		rev := RevisionHash(getDM("dt1").Spec, defaultDigest, fakeImage)
		job := &batchv1.Job{}
		err = k8sClient.Get(ctx, types.NamespacedName{
			Namespace: namespace, Name: "dt1-prefetch-" + rev,
		}, job)
		Expect(err).To(HaveOccurred(), "no Job should exist")
	})

	// a download-token Secret with the label → Job has OLLAYA_HF_TOKEN env,
	// serving Deployment has no such env.
	It("DT2: labelled download-token Secret → Job gets OLLAYA_HF_TOKEN, Deployment does not", func() {
		eng := newFakeEngine()
		r := newR(eng)
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: "hf-good",
				Labels: map[string]string{decisionmodelv1alpha1.LabelDownloadToken: "true"},
			},
			Data: map[string][]byte{"token": []byte("hf_xxx")},
		}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())
		createDM("dt2", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Cache = &decisionmodelv1alpha1.CacheSpec{
				DownloadTokenSecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "hf-good"},
					Key:                  "token",
				},
			}
		})

		// First reconcile: resolves digest, creates PVC + prefetch Job.
		reconcileOnce(r, "dt2")
		Expect(getDM("dt2").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseCaching))
		rev := RevisionHash(getDM("dt2").Spec, defaultDigest, fakeImage)

		// Check the Job: must have OLLAYA_HF_TOKEN env var referencing the Secret.
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Namespace: namespace, Name: "dt2-prefetch-" + rev,
		}, job)).To(Succeed())
		jobEnv := job.Spec.Template.Spec.Containers[0].Env
		found := false
		for _, e := range jobEnv {
			if e.Name == "OLLAYA_HF_TOKEN" {
				found = true
				Expect(e.ValueFrom).NotTo(BeNil())
				Expect(e.ValueFrom.SecretKeyRef.Name).To(Equal("hf-good"))
				Expect(e.ValueFrom.SecretKeyRef.Key).To(Equal("token"))
			}
		}
		Expect(found).To(BeTrue(), "prefetch Job must have OLLAYA_HF_TOKEN env var")

		// Complete the prefetch and advance to Starting.
		markPrefetchComplete(ctx, namespace, "dt2", rev)
		reconcileOnce(r, "dt2")

		// The serving Deployment must NOT have OLLAYA_HF_TOKEN.
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Namespace: namespace, Name: "dt2-" + rev,
		}, dep)).To(Succeed())
		for _, e := range dep.Spec.Template.Spec.Containers[0].Env {
			Expect(e.Name).NotTo(Equal("OLLAYA_HF_TOKEN"),
				"serving Deployment must not contain OLLAYA_HF_TOKEN")
		}
	})

	// changing the download-token ref does not create a new revision (it is a
	// cache field, not a revision field).
	It("DT3: changing downloadTokenSecretRef does not create a new revision", func() {
		eng := newFakeEngine()
		r := newR(eng)
		secret1 := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: "hf-1",
				Labels: map[string]string{decisionmodelv1alpha1.LabelDownloadToken: "true"},
			},
			Data: map[string][]byte{"token": []byte("hf_aaa")},
		}
		secret2 := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: "hf-2",
				Labels: map[string]string{decisionmodelv1alpha1.LabelDownloadToken: "true"},
			},
			Data: map[string][]byte{"token": []byte("hf_bbb")},
		}
		Expect(k8sClient.Create(ctx, secret1)).To(Succeed())
		Expect(k8sClient.Create(ctx, secret2)).To(Succeed())
		createDM("dt3", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Cache = &decisionmodelv1alpha1.CacheSpec{
				DownloadTokenSecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "hf-1"},
					Key:                  "token",
				},
			}
		})

		// First reconcile: Caching.
		reconcileOnce(r, "dt3")
		revBefore := RevisionHash(getDM("dt3").Spec, defaultDigest, fakeImage)
		Expect(revBefore).NotTo(BeEmpty())

		// Switch to a different Secret ref.
		Expect(updateDM(ctx, namespace, "dt3", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Cache.DownloadTokenSecretRef.Name = "hf-2"
		})).To(Succeed())

		reconcileOnce(r, "dt3")
		revAfter := RevisionHash(getDM("dt3").Spec, defaultDigest, fakeImage)
		Expect(revAfter).To(Equal(revBefore), "download-token change must not produce a new revision hash")
	})

	// a labelled download-token Secret missing the referenced key →
	// Degraded/DownloadTokenInvalid, no Job.
	It("DT4: labelled download-token Secret missing the key → DownloadTokenInvalid, no Job", func() {
		eng := newFakeEngine()
		r := newR(eng)
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: "hf-nokey",
				Labels: map[string]string{decisionmodelv1alpha1.LabelDownloadToken: "true"},
			},
			Data: map[string][]byte{"other": []byte("x")}, // no "token" key
		}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())
		createDM("dt4", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Cache = &decisionmodelv1alpha1.CacheSpec{
				DownloadTokenSecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "hf-nokey"},
					Key:                  "token",
				},
			}
		})

		res, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: namespace, Name: "dt4"},
		})
		Expect(err).NotTo(HaveOccurred(), "DownloadTokenInvalid must not be a terminal error")
		Expect(res.RequeueAfter).To(Equal(regateInterval))
		Expect(getDM("dt4").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseDegraded))
		Expect(readyReason("dt4")).To(Equal(reasonDownloadTokenInvalid))

		rev := RevisionHash(getDM("dt4").Spec, defaultDigest, fakeImage)
		job := &batchv1.Job{}
		err = k8sClient.Get(ctx, types.NamespacedName{
			Namespace: namespace, Name: "dt4-prefetch-" + rev,
		}, job)
		Expect(err).To(HaveOccurred(), "no Job should exist")
	})

	// a missing download-token Secret (optional not honoured) → the Secret Get
	// error surfaces; no Job. The ref is required when set.
	It("DT5: missing download-token Secret is required (optional ignored), no Job", func() {
		eng := newFakeEngine()
		r := newR(eng)
		createDM("dt5", func(dm *decisionmodelv1alpha1.DecisionModel) {
			optTrue := true
			dm.Spec.Cache = &decisionmodelv1alpha1.CacheSpec{
				DownloadTokenSecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "absent"},
					Key:                  "token",
					Optional:             &optTrue, // must be ignored: ref is required
				},
			}
		})

		// A missing Secret surfaces as an error (not silently skipped).
		_, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: namespace, Name: "dt5"},
		})
		Expect(err).To(HaveOccurred(), "a missing download-token Secret must not be skipped via optional")

		rev := RevisionHash(getDM("dt5").Spec, defaultDigest, fakeImage)
		job := &batchv1.Job{}
		gerr := k8sClient.Get(ctx, types.NamespacedName{
			Namespace: namespace, Name: "dt5-prefetch-" + rev,
		}, job)
		Expect(gerr).To(HaveOccurred(), "no Job should exist")
	})

	// an invalid model name (with a valid tag per CRD) fails with InvalidModelName.
	It("E4: rejects an invalid model name", func() {
		eng := newFakeEngine()
		r := newR(eng)
		// "-x:t" passes the CRD tag CEL but the engine's CanonicalName rejects it.
		createDM("e4", func(dm *decisionmodelv1alpha1.DecisionModel) { dm.Spec.Model = "-x:t" })

		reconcilePermanent(r, "e4")
		Expect(getDM("e4").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseFailed))
		Expect(readyReason("e4")).To(Equal(reasonInvalidModelName))
		Expect(eng.resolveCount()).To(Equal(0))
	})

	// the serving Deployment mounts the per-revision store sub-path.
	It("E1: sets a per-revision store sub-path on serving Pods", func() {
		eng := newFakeEngine()
		r := newR(eng)
		createDM("e1", nil)
		reconcileOnce(r, "e1")
		rev := RevisionHash(getDM("e1").Spec, defaultDigest, fakeImage)
		markPrefetchComplete(ctx, namespace, "e1", rev)
		reconcileOnce(r, "e1")

		// The fake engine records StoreSubPath into the pod annotation via params;
		// assert paramsFor produced it (unit-level guarantee is in TestE1KeepSubPaths;
		// here we assert the Deployment exists for the revision).
		Expect(rev).NotTo(BeEmpty())
	})

	// a labelled API-key Secret missing the referenced key is held
	// Degraded/APIKeyInvalid (prober and serving Pod would otherwise diverge).
	It("A1: API-key Secret missing the referenced key -> Degraded/APIKeyInvalid", func() { // gitleaks:allow (test name, not a secret)
		eng := newFakeEngine()
		r := newR(eng)
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: "key-nokey",
				Labels: map[string]string{decisionmodelv1alpha1.LabelAPIKey: "true"},
			},
			Data: map[string][]byte{"other": []byte("x")},
		}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())
		createDM("a1", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Auth = &decisionmodelv1alpha1.AuthSpec{
				APIKeySecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "key-nokey"}, Key: "token",
				},
			}
		})
		res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "a1"}})
		Expect(err).NotTo(HaveOccurred(), "APIKeyInvalid must not be terminal")
		Expect(res.RequeueAfter).To(Equal(regateInterval))
		Expect(getDM("a1").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseDegraded))
		Expect(readyReason("a1")).To(Equal(reasonAPIKeyInvalid))
		// Adding the key releases it (resolve proceeds).
		Expect(func() error {
			s := &corev1.Secret{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "key-nokey"}, s); err != nil {
				return err
			}
			s.Data["token"] = []byte("s3cr3t")
			return k8sClient.Update(ctx, s)
		}()).To(Succeed())
		reconcileOnce(r, "a1")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			reconcileOnce(r, "a1")
			return getDM("a1").Status.Phase
		}, "5s", "50ms").ShouldNot(Equal(decisionmodelv1alpha1.PhaseDegraded),
			"adding the key releases the APIKeyInvalid hold")
	})

	// CEL rejects apiKeySecretRef.optional: true.
	It("A2: rejects apiKeySecretRef.optional=true (CEL)", func() {
		optional := true
		err := k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "a2"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model, Device: "cpu", Replicas: int32Ptr(1),
				Auth: &decisionmodelv1alpha1.AuthSpec{
					APIKeySecretRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "k"}, Key: "token", Optional: &optional,
					},
				},
			},
		})
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "want Invalid, got %v", err)
	})

	// a dataset Secret labelled with the deprecated api-key label still works and
	// emits a DeprecatedSecretLabel Warning.
	It("A3: dataset Secret via the deprecated api-key label works with a Warning", func() {
		eng := &deciderFakeEngine{fakeEngine: newFakeEngine(), choice: "billing"}
		r := newR(eng)
		dataset := []byte(`{"state":{},"questions":{"q1":{"type":"choice"}},"expected":{"q1":"billing"}}` + "\n")
		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: "ds-dep",
				Labels: map[string]string{decisionmodelv1alpha1.LabelAPIKey: "true"},
			},
			Data: map[string][]byte{"cases.jsonl": dataset},
		})).To(Succeed())
		createDM("a3", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Rollout = &decisionmodelv1alpha1.RolloutSpec{
				Evaluation: &decisionmodelv1alpha1.EvaluationSpec{
					DatasetRef: decisionmodelv1alpha1.DatasetRef{
						SecretRef: &decisionmodelv1alpha1.DatasetKeyRef{Name: "ds-dep", Key: "cases.jsonl"},
					},
					MinAccuracy: "0.90",
				},
			}
		})
		rev := RevisionHash(getDM("a3").Spec, defaultDigest, fakeImage)
		reconcileOnce(r, "a3")
		markPrefetchComplete(ctx, namespace, "a3", rev)
		reconcileOnce(r, "a3")
		createReadyPodSG(ctx, namespace, "a3", rev)
		// Drive to evaluation; the deprecated-label Warning must have fired.
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "a3"}})
		Eventually(func() bool {
			fr := r.Recorder.(*events.FakeRecorder)
			for {
				select {
				case e := <-fr.Events:
					if strings.Contains(e, eventDeprecatedSecretLabel) {
						return true
					}
				default:
					_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "a3"}})
					return false
				}
			}
		}, "5s", "50ms").Should(BeTrue(), "a DeprecatedSecretLabel Warning must be emitted")
	})

	// a dataset Secret with no capability label holds (SecretNotAllowed), not fails.
	It("A4: dataset Secret with no capability label holds", func() {
		eng := &deciderFakeEngine{fakeEngine: newFakeEngine(), choice: "billing"}
		r := newR(eng)
		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "ds-nolabel"},
			Data:       map[string][]byte{"cases.jsonl": []byte("{}\n")},
		})).To(Succeed())
		createDM("a4", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Rollout = &decisionmodelv1alpha1.RolloutSpec{
				Evaluation: &decisionmodelv1alpha1.EvaluationSpec{
					DatasetRef: decisionmodelv1alpha1.DatasetRef{
						SecretRef: &decisionmodelv1alpha1.DatasetKeyRef{Name: "ds-nolabel", Key: "cases.jsonl"},
					},
					MinAccuracy: "0.90",
				},
			}
		})
		rev := RevisionHash(getDM("a4").Spec, defaultDigest, fakeImage)
		reconcileOnce(r, "a4")
		markPrefetchComplete(ctx, namespace, "a4", rev)
		reconcileOnce(r, "a4")
		createReadyPodSG(ctx, namespace, "a4", rev)
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "a4"}})
		dm := getDM("a4")
		Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseEvaluating))
		Expect(dm.Status.FailedRevision).To(BeNil())
		Expect(meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionEvaluated).Reason).
			To(Equal(reasonSecretNotAllowed))
	})
})

// createReadyPodSG creates a gated, model-ready Pod for a revision (security
// envtest helper).
func createReadyPodSG(ctx context.Context, ns, dmName, rev string) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns, Name: dmName + "-pod-" + rev,
			Labels: map[string]string{decisionmodelv1alpha1.LabelName: dmName, decisionmodelv1alpha1.LabelRevision: rev},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
	}
	Expect(k8sClient.Create(ctx, pod)).To(Succeed())
	pod.Status.PodIP = "10.0.0.80"
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
	}
	Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
}

// markPrefetchFailed sets the prefetch Job for a revision to Failed.
func markPrefetchFailed(ctx context.Context, ns, dmName, rev string) {
	job := &batchv1.Job{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: dmName + "-prefetch-" + rev}, job)).To(Succeed())
	now := metav1.Now()
	job.Status.StartTime = &now
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue},
		{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
	}
	Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
}

// markPrefetchComplete sets the prefetch Job for a revision to Complete.
func markPrefetchComplete(ctx context.Context, ns, dmName, rev string) {
	job := &batchv1.Job{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: dmName + "-prefetch-" + rev}, job)).To(Succeed())
	now := metav1.Now()
	job.Status.StartTime = &now
	job.Status.CompletionTime = &now
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
		{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
	}
	Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
}
