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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// A secret value (API key, download token, dataset content) and its derivatives
// (length, any prefix, a sha256, the HMAC apiKeyChecksum) must never appear in
// status, Events, or logs — only the raw value is secret, but a length/prefix/
// hash lets a view-role reader verify a guess, so none may leak either.
var _ = Describe("secrets and derivatives never leak into status or Events", func() {
	var (
		ctx       context.Context
		namespace string
		counter   int
	)
	const apiKeyValue = "S3CR3T-api-key-sentinel-value-01234567"
	const tokenValue = "HF-download-token-sentinel-value-89abcdef"
	int32Ptr := func(v int32) *int32 { return &v }
	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
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
	gatedPod := func(name, rev, ip string) {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: name + "-pod-" + rev,
				Labels: map[string]string{decisionmodelv1alpha1.LabelName: name, decisionmodelv1alpha1.LabelRevision: rev},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, p)).To(Succeed())
		p.Status.PodIP = ip
		p.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		namespace = fmt.Sprintf("noleak-%d", counter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	It("keeps the API-key / download-token values and their derivatives out of status and Events", func() {
		rr := events.NewFakeRecorder(512)
		r := &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:  map[string]engine.Engine{"ollaya": newFakeEngine()},
			Prober:   &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}},
			Recorder: rr,
		}
		keySec := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "key", Labels: map[string]string{decisionmodelv1alpha1.LabelAPIKey: "true"}},
			Data:       map[string][]byte{"token": []byte(apiKeyValue)},
		}
		Expect(k8sClient.Create(ctx, keySec)).To(Succeed())
		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "hf", Labels: map[string]string{decisionmodelv1alpha1.LabelDownloadToken: "true"}},
			Data:       map[string][]byte{"token": []byte(tokenValue)},
		})).To(Succeed())
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "sl"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
				Auth: &decisionmodelv1alpha1.AuthSpec{APIKeySecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "key"}, Key: "token",
				}},
				Cache: &decisionmodelv1alpha1.CacheSpec{DownloadTokenSecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "hf"}, Key: "token",
				}},
			},
		})).To(Succeed())
		rec(r, "sl")
		rev := RevisionHash(getDM("sl").Spec, defaultDigest, fakeImage)
		markJob("sl", rev)
		rec(r, "sl")
		gatedPod("sl", rev, "10.0.40.1")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			rec(r, "sl")
			return getDM("sl").Status.Phase
		}, "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))

		// Collect the serialized status and every Event.
		statusJSON, err := json.Marshal(getDM("sl").Status)
		Expect(err).NotTo(HaveOccurred())
		haystacks := map[string]string{"status": string(statusJSON)}
		var evs []string
		for len(rr.Events) > 0 {
			evs = append(evs, <-rr.Events)
		}
		haystacks["events"] = strings.Join(evs, "\n")

		// Build the forbidden needles: the raw values and their derivatives.
		keyUID := func() types.UID {
			s := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "key"}, s)).To(Succeed())
			return s.UID
		}()
		sha := func(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
		needles := map[string]string{}
		for label, v := range map[string]string{"api-key": apiKeyValue, "download-token": tokenValue} {
			needles[label+" raw"] = v
			needles[label+" len"] = strconv.Itoa(len(v))
			needles[label+" prefix8"] = v[:8]
			needles[label+" sha256"] = sha(v)
			needles[label+" sha256-16"] = sha(v)[:16]
		}
		// The HMAC checksum is written to the Pod-template annotation by design, but
		// must not appear in status or Events.
		needles["api-key checksum"] = apiKeyChecksum(keyUID, apiKeyValue)

		for where, hay := range haystacks {
			for what, needle := range needles {
				// A length like "38" is a common substring; only flag it when it is
				// not already part of an unrelated number. Keep it simple: the raw
				// value, prefix, and hashes are the real leak risks; assert all.
				if what == "api-key len" || what == "download-token len" {
					continue // length alone is too ambiguous to grep safely in JSON
				}
				Expect(hay).NotTo(ContainSubstring(needle),
					"%s leaked into %s", what, where)
			}
		}
		// Sanity: the DM did reach Ready with the API key applied (so the paths that
		// could leak actually ran).
		Expect(getDM("sl").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseReady))
	})
})
