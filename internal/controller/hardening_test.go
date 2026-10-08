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
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// Every name the operator derives from a max-length DecisionModel name must be
// valid for its Kubernetes kind, and one more character must break the tightest
// one (the prefetch Job, whose name is a Pod label value via job-name).
func TestDerivedNamesForMaxLengthDM(t *testing.T) {
	dmFor := func(n int) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		dm.Name = strings.Repeat("a", n)
		return dm
	}
	spec := decisionmodelv1alpha1.DecisionModelSpec{Engine: "ollaya", Model: "laya:en", Device: "cpu"}
	rev := RevisionHash(spec, defaultDigest, fakeImage)
	if len(rev) != 16 {
		t.Fatalf("revision hash length = %d, want 16 (MaxNameLength assumes 16)", len(rev))
	}
	legacyRev := legacyRevisionHash(spec, defaultDigest, fakeImage) // 10-hex, old revisions

	dm := dmFor(decisionmodelv1alpha1.MaxNameLength)
	tests := []struct {
		name  string
		value string
		check func(string) []string
	}{
		{"service", dm.Name, validation.IsDNS1035Label},
		{"deployment", revisionName(dm, rev), validation.IsDNS1123Label},
		{"prefetch job (16-hex, -pf-)", prefetchName(dm, rev), validation.IsDNS1123Label},
		{"prefetch job (10-hex, -prefetch-)", prefetchName(dm, legacyRev), validation.IsDNS1123Label},
		// PVC names are DNS-1123 subdomains (<= 253), not labels; a 16-hex store
		// name is 66 chars, which is valid for a PVC (only the prefetch Job name is
		// bound by the 63-char job-name label).
		{"store pvc", storeNameRev(dm, rev), validation.IsDNS1123Subdomain},
		{"legacy store pvc", storeName(dm), validation.IsDNS1123Subdomain},
		{"pdb", pdbName(dm, rev), validation.IsDNS1123Label},
		{"name label value", dm.Name, validation.IsValidLabelValue},
		{"job-name label value (16-hex)", prefetchName(dm, rev), validation.IsValidLabelValue},
		{"job-name label value (10-hex)", prefetchName(dm, legacyRev), validation.IsValidLabelValue},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if errs := tt.check(tt.value); len(errs) != 0 {
				t.Errorf("%q (len %d) invalid: %v", tt.value, len(tt.value), errs)
			}
		})
	}

	// Both prefetch Job name forms are exactly at the 63-char job-name limit for a
	// MaxNameLength DM, and one more character overflows either form.
	if n := len(prefetchName(dm, rev)); n != 63 {
		t.Errorf("16-hex prefetch name is %d chars, want 63 (tight)", n)
	}
	if n := len(prefetchName(dm, legacyRev)); n != 63 {
		t.Errorf("10-hex prefetch name is %d chars, want 63 (tight)", n)
	}
	if got := prefetchName(dmFor(decisionmodelv1alpha1.MaxNameLength+1), rev); len(got) <= 63 {
		t.Errorf("16-hex prefetch name for MaxNameLength+1 is %d chars; not tight", len(got))
	}
	if got := prefetchName(dmFor(decisionmodelv1alpha1.MaxNameLength+1), legacyRev); len(got) <= 63 {
		t.Errorf("10-hex prefetch name for MaxNameLength+1 is %d chars; not tight", len(got))
	}
}

func TestProxyEnvFromEnviron(t *testing.T) {
	tests := []struct {
		name        string
		env         map[string]string
		want        []corev1.EnvVar
		wantSkipped []string
	}{
		{name: "unset", env: map[string]string{}},
		{name: "empty values ignored", env: map[string]string{"HTTP_PROXY": ""}},
		{
			name: "upper and lower case, fixed order",
			env:  map[string]string{"no_proxy": "lo", "HTTPS_PROXY": "http://p:3128", "http_proxy": "http://q:3128"},
			want: []corev1.EnvVar{
				{Name: "HTTPS_PROXY", Value: "http://p:3128"},
				{Name: "http_proxy", Value: "http://q:3128"},
				{Name: "no_proxy", Value: "lo"},
			},
		},
		{
			name: "unrelated keys never copied",
			env:  map[string]string{"ALL_PROXY": "x", "KUBECONFIG": "y", "HTTP_PROXY": "http://p"},
			want: []corev1.EnvVar{{Name: "HTTP_PROXY", Value: "http://p"}},
		},

		// credentials never reach a tenant Job.
		{
			name: "plain http proxy is copied",
			env:  map[string]string{"HTTP_PROXY": "http://proxy.corp:3128"},
			want: []corev1.EnvVar{{Name: "HTTP_PROXY", Value: "http://proxy.corp:3128"}},
		},
		{
			name: "plain https proxy with a path is copied",
			env:  map[string]string{"HTTPS_PROXY": "https://proxy.corp:3129/"},
			want: []corev1.EnvVar{{Name: "HTTPS_PROXY", Value: "https://proxy.corp:3129/"}},
		},
		{
			name:        "user:pass@ is skipped",
			env:         map[string]string{"HTTPS_PROXY": "http://svc:s3cr3t@proxy.corp:3128"},
			wantSkipped: []string{"HTTPS_PROXY"},
		},
		{
			name:        "user@ (no password) is skipped",
			env:         map[string]string{"HTTP_PROXY": "http://svc@proxy.corp:3128"},
			wantSkipped: []string{"HTTP_PROXY"},
		},
		{
			name:        "empty user with a password is skipped",
			env:         map[string]string{"HTTP_PROXY": "http://:s3cr3t@proxy.corp:3128"},
			wantSkipped: []string{"HTTP_PROXY"},
		},
		{
			name:        "scheme-less user:pass@host is skipped (plain url.Parse would hide the userinfo)",
			env:         map[string]string{"HTTPS_PROXY": "svc:s3cr3t@proxy.corp:3128"},
			wantSkipped: []string{"HTTPS_PROXY"},
		},
		{
			name: "scheme-less host:port without credentials is copied",
			env:  map[string]string{"HTTP_PROXY": "proxy.corp:3128"},
			want: []corev1.EnvVar{{Name: "HTTP_PROXY", Value: "proxy.corp:3128"}},
		},
		{
			name: "scheme-less localhost:port is copied (not read as scheme 'localhost')",
			env:  map[string]string{"HTTP_PROXY": "localhost:3128"},
			want: []corev1.EnvVar{{Name: "HTTP_PROXY", Value: "localhost:3128"}},
		},
		{
			name:        "socks5 with credentials is skipped",
			env:         map[string]string{"HTTPS_PROXY": "socks5://svc:s3cr3t@proxy.corp:1080"},
			wantSkipped: []string{"HTTPS_PROXY"},
		},
		{
			name: "socks5 without credentials is copied",
			env:  map[string]string{"HTTPS_PROXY": "socks5://proxy.corp:1080"},
			want: []corev1.EnvVar{{Name: "HTTPS_PROXY", Value: "socks5://proxy.corp:1080"}},
		},
		{
			// Regression for a real fail-open found while writing this test: a
			// credentialed value that does not parse must still be skipped. A retry
			// with an "http://" prefix used to "parse" it with the userinfo hidden in
			// the path.
			name:        "credentialed AND unparsable (space in userinfo) is skipped",
			env:         map[string]string{"HTTP_PROXY": "http://a b:p@host:1"},
			wantSkipped: []string{"HTTP_PROXY"},
		},
		{
			name:        "credentialed scheme-less AND unparsable is skipped",
			env:         map[string]string{"HTTPS_PROXY": "a b:p@host:1"},
			wantSkipped: []string{"HTTPS_PROXY"},
		},
		{
			name:        "percent-encoded credentials are skipped",
			env:         map[string]string{"HTTP_PROXY": "http://svc:p%40ss@proxy.corp:3128"},
			wantSkipped: []string{"HTTP_PROXY"},
		},
		{
			name: "IPv6 literal without credentials is copied",
			env:  map[string]string{"HTTP_PROXY": "http://[2001:db8::1]:3128"},
			want: []corev1.EnvVar{{Name: "HTTP_PROXY", Value: "http://[2001:db8::1]:3128"}},
		},
		{
			// Decision: an unparsable value cannot be proven free of credentials and
			// the operator's own HTTP client cannot use it either, so it is skipped.
			name:        "unparsable value is skipped",
			env:         map[string]string{"HTTP_PROXY": "http://pro xy:3128"},
			wantSkipped: []string{"HTTP_PROXY"},
		},
		{
			name:        "invalid escape is skipped",
			env:         map[string]string{"HTTPS_PROXY": "http://proxy.corp:3128/%zz"},
			wantSkipped: []string{"HTTPS_PROXY"},
		},
		{
			name: "NO_PROXY is always copied",
			env:  map[string]string{"NO_PROXY": "localhost,.svc,10.0.0.0/8"},
			want: []corev1.EnvVar{{Name: "NO_PROXY", Value: "localhost,.svc,10.0.0.0/8"}},
		},
		{
			name: "NO_PROXY is copied even when it looks like credentials",
			env:  map[string]string{"no_proxy": "a:b@c"},
			want: []corev1.EnvVar{{Name: "no_proxy", Value: "a:b@c"}},
		},
		{
			name: "mixed: credentialed lower-case skipped, plain upper-case and NO_PROXY copied, names in fixed order",
			env: map[string]string{
				"HTTP_PROXY": "http://proxy.corp:3128", "NO_PROXY": "localhost",
				"https_proxy": "http://svc:s3cr3t@proxy.corp:3128", "HTTPS_PROXY": "http://u:p@proxy.corp:3128",
			},
			want: []corev1.EnvVar{
				{Name: "HTTP_PROXY", Value: "http://proxy.corp:3128"},
				{Name: "NO_PROXY", Value: "localhost"},
			},
			wantSkipped: []string{"HTTPS_PROXY", "https_proxy"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, skipped := ProxyEnvFromEnviron(func(k string) string { return tt.env[k] })
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("env = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(skipped, tt.wantSkipped) {
				t.Errorf("skipped = %v, want %v", skipped, tt.wantSkipped)
			}
			// Skipped holds key names only.
			for _, k := range skipped {
				if strings.ContainsAny(k, ":/@") {
					t.Errorf("skipped entry %q looks like a value, not a key name", k)
				}
			}
		})
	}
}

// End to end: whatever the operator's environment holds, no credential appears
// anywhere in the serialized prefetch PodSpec.
func TestProxyCredentialsNeverReachThePrefetchPodSpec(t *testing.T) {
	secrets := []string{"s3cr3t-pw", "svc-acct"}
	env := map[string]string{
		"HTTPS_PROXY": "http://svc-acct:s3cr3t-pw@proxy.corp:3128",
		"https_proxy": "svc-acct:s3cr3t-pw@proxy.corp:3128", // scheme-less form
		"HTTP_PROXY":  "http://proxy.corp:3128",
		"NO_PROXY":    "localhost,.svc",
	}
	proxyEnv, _ := ProxyEnvFromEnviron(func(k string) string { return env[k] })
	spec := &corev1.PodSpec{Containers: []corev1.Container{{Name: "prefetch"}}}
	applyProxyEnv(spec, proxyEnv)

	b, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range secrets {
		if strings.Contains(string(b), s) {
			t.Errorf("credential fragment %q leaked into the prefetch PodSpec: %s", s, b)
		}
	}
	names := map[string]bool{}
	for _, e := range spec.Containers[0].Env {
		names[e.Name] = true
	}
	if !names["HTTP_PROXY"] || !names["NO_PROXY"] {
		t.Errorf("the shareable settings must still be passed, got %v", names)
	}
}

func TestApplyProxyEnv(t *testing.T) {
	proxy := []corev1.EnvVar{
		{Name: "HTTPS_PROXY", Value: "http://p:3128"},
		{Name: "NO_PROXY", Value: "svc.local"},
	}
	tests := []struct {
		name      string
		existing  []corev1.EnvVar
		proxy     []corev1.EnvVar
		wantNames []string
		wantValue map[string]string
	}{
		{"no proxy configured", []corev1.EnvVar{{Name: "A", Value: "1"}}, nil, []string{"A"}, nil},
		{"added when absent", []corev1.EnvVar{{Name: "A", Value: "1"}}, proxy,
			[]string{"A", "HTTPS_PROXY", "NO_PROXY"}, map[string]string{"HTTPS_PROXY": "http://p:3128"}},
		{"existing key wins", []corev1.EnvVar{{Name: "NO_PROXY", Value: "mine"}}, proxy,
			[]string{"NO_PROXY", "HTTPS_PROXY"}, map[string]string{"NO_PROXY": "mine"}},
		{"lower-case spelling already set: upper-case not added", []corev1.EnvVar{{Name: "no_proxy", Value: "mine"}}, proxy,
			[]string{"no_proxy", "HTTPS_PROXY"}, map[string]string{"no_proxy": "mine"}},
		{"upper-case already set: lower-case operator value not added",
			[]corev1.EnvVar{{Name: "https_proxy", Value: "http://mine:1"}}, proxy,
			[]string{"https_proxy", "NO_PROXY"}, map[string]string{"https_proxy": "http://mine:1"}},
		{"both spellings from the operator are added together when the container has neither",
			[]corev1.EnvVar{{Name: "A", Value: "1"}},
			[]corev1.EnvVar{{Name: "HTTPS_PROXY", Value: "u"}, {Name: "https_proxy", Value: "l"}},
			[]string{"A", "HTTPS_PROXY", "https_proxy"}, map[string]string{"HTTPS_PROXY": "u", "https_proxy": "l"}},
		{"a different variable is unaffected by another pair's presence",
			[]corev1.EnvVar{{Name: "HTTP_PROXY", Value: "x"}}, proxy,
			[]string{"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY"}, map[string]string{"HTTP_PROXY": "x"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := &corev1.PodSpec{Containers: []corev1.Container{{Name: "prefetch", Env: tt.existing}}}
			applyProxyEnv(spec, tt.proxy)
			var names []string
			vals := map[string]string{}
			for _, e := range spec.Containers[0].Env {
				names = append(names, e.Name)
				vals[e.Name] = e.Value
			}
			if !reflect.DeepEqual(names, tt.wantNames) {
				t.Errorf("env names = %v, want %v", names, tt.wantNames)
			}
			for k, v := range tt.wantValue {
				if vals[k] != v {
					t.Errorf("%s = %q, want %q", k, vals[k], v)
				}
			}
		})
	}
}

func TestApplyRuntimeClass(t *testing.T) {
	nvidia := "nvidia"
	tests := []struct {
		name string
		s    *decisionmodelv1alpha1.SchedulingSpec
		want *string
	}{
		{"nil scheduling", nil, nil},
		{"unset", &decisionmodelv1alpha1.SchedulingSpec{}, nil},
		{"set", &decisionmodelv1alpha1.SchedulingSpec{RuntimeClassName: &nvidia}, &nvidia},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := &corev1.PodSpec{}
			applyRuntimeClass(spec, tt.s)
			if !reflect.DeepEqual(spec.RuntimeClassName, tt.want) {
				t.Errorf("RuntimeClassName = %v, want %v", spec.RuntimeClassName, tt.want)
			}
		})
	}
}

var _ = Describe("hardening", func() {
	var (
		ctx       context.Context
		namespace string
		nsCounter int
	)

	int32Ptr := func(v int32) *int32 { return &v }

	newR := func() *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client:    k8sClient,
			APIReader: k8sClient,
			Scheme:    k8sClient.Scheme(),
			Engines:   map[string]engine.Engine{"ollaya": newFakeEngine()},
			Prober:    &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}},
			Recorder:  events.NewFakeRecorder(64),
		}
	}

	reconcileOnce := func(r *DecisionModelReconciler, name string) {
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
		Expect(err).NotTo(HaveOccurred())
	}

	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}

	newDM := func(name string, mutate func(*decisionmodelv1alpha1.DecisionModel)) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		}
		if mutate != nil {
			mutate(dm)
		}
		return dm
	}

	markJobComplete := func(dmName, rev string) {
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName(dmName, rev)}, job)).To(Succeed())
		now := metav1.Now()
		job.Status.StartTime = &now
		job.Status.CompletionTime = &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}

	createGatedPod := func(dmName, rev, podName string) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: podName,
				Labels: map[string]string{
					decisionmodelv1alpha1.LabelName:     dmName,
					decisionmodelv1alpha1.LabelRevision: rev,
				},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.PodIP = "10.0.0.40"
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue}, // what the kubelet sets once containers and gate are ready
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = fmt.Sprintf("hardening-test-%d", nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	Describe("name validation (CEL)", func() {
		It("accepts a name of exactly MaxNameLength", func() {
			name := strings.Repeat("a", decisionmodelv1alpha1.MaxNameLength)
			Expect(k8sClient.Create(ctx, newDM(name, nil))).To(Succeed())
		})

		DescribeTable("rejects invalid names",
			func(name, wantMsg string) {
				err := k8sClient.Create(ctx, newDM(name, nil))
				Expect(err).To(HaveOccurred())
				Expect(apierrors.IsInvalid(err)).To(BeTrue(), "got %v", err)
				Expect(err.Error()).To(ContainSubstring(wantMsg))
			},
			Entry("one char too long", strings.Repeat("a", decisionmodelv1alpha1.MaxNameLength+1), "at most 43 characters"),
			Entry("dots", "laya.en", "DNS-1035 label"),
			Entry("leading digit", "9abc", "DNS-1035 label"),
		)
	})

	Describe("scheduling.runtimeClassName", func() {
		It("is set on serving Pods only, and changing it starts a new revision", func() {
			r := newR()
			nvidia := "nvidia"
			Expect(k8sClient.Create(ctx, newDM("rc1", func(dm *decisionmodelv1alpha1.DecisionModel) {
				dm.Spec.Scheduling = &decisionmodelv1alpha1.SchedulingSpec{RuntimeClassName: &nvidia}
			}))).To(Succeed())

			reconcileOnce(r, "rc1")
			rev := RevisionHash(getDM("rc1").Spec, defaultDigest, fakeImage)
			job := &batchv1.Job{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("rc1", rev)}, job)).To(Succeed())
			Expect(job.Spec.Template.Spec.RuntimeClassName).To(BeNil(), "prefetch Job needs no GPU runtime")

			markJobComplete("rc1", rev)
			reconcileOnce(r, "rc1")
			createGatedPod("rc1", rev, "rc1-pod-0")
			reconcileOnce(r, "rc1") // probe -> promote
			reconcileOnce(r, "rc1") // stable path
			Expect(getDM("rc1").Status.StableRevision).NotTo(BeNil())

			depKey := types.NamespacedName{Namespace: namespace, Name: "rc1-" + rev}
			dep := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, depKey, dep)).To(Succeed())
			Expect(dep.Spec.Template.Spec.RuntimeClassName).To(Equal(&nvidia))

			// Change it: this is a NEW revision (blue-green, own store),
			// not an in-place roll of the running stable.
			stableDepRV := dep.ResourceVersion
			Expect(updateDM(ctx, namespace, "rc1", func(dm *decisionmodelv1alpha1.DecisionModel) {
				other := "gpu-runtime"
				dm.Spec.Scheduling.RuntimeClassName = &other
			})).To(Succeed())
			newRev := RevisionHash(getDM("rc1").Spec, defaultDigest, fakeImage)
			Expect(newRev).NotTo(Equal(rev), "runtimeClassName is part of the revision identity")
			reconcileOnce(r, "rc1")

			dm := getDM("rc1")
			Expect(dm.Status.StableRevision.Hash).To(Equal(rev), "the stable keeps serving")
			Expect(dm.Status.CandidateRevision).NotTo(BeNil())
			Expect(dm.Status.CandidateRevision.Hash).To(Equal(newRev))
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("rc1", newRev)}, &batchv1.Job{})).
				To(Succeed(), "the candidate gets its own prefetch Job")
			Expect(k8sClient.Get(ctx, depKey, dep)).To(Succeed())
			Expect(dep.ResourceVersion).To(Equal(stableDepRV), "the running stable Deployment is not touched")
			Expect(*dep.Spec.Template.Spec.RuntimeClassName).To(Equal(nvidia))
		})
	})

	Describe("prefetch proxy env", func() {
		It("reaches the prefetch Job but not the serving Deployment", func() {
			r := newR()
			r.PrefetchProxyEnv = []corev1.EnvVar{{Name: "HTTPS_PROXY", Value: "http://proxy.corp:3128"}}
			Expect(k8sClient.Create(ctx, newDM("px1", nil))).To(Succeed())

			reconcileOnce(r, "px1")
			rev := RevisionHash(getDM("px1").Spec, defaultDigest, fakeImage)
			job := &batchv1.Job{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("px1", rev)}, job)).To(Succeed())
			Expect(job.Spec.Template.Spec.Containers[0].Env).To(ContainElement(
				corev1.EnvVar{Name: "HTTPS_PROXY", Value: "http://proxy.corp:3128"}))

			markJobComplete("px1", rev)
			reconcileOnce(r, "px1")
			dep := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "px1-" + rev}, dep)).To(Succeed())
			for _, e := range dep.Spec.Template.Spec.Containers[0].Env {
				Expect(e.Name).NotTo(Equal("HTTPS_PROXY"), "serving Pods never get the proxy env")
			}
		})
	})
})
