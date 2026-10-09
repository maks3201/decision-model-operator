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

// Package rbac holds a guard test over the operator's generated ClusterRole. It
// is deliberately a plain `go test` with no cluster or envtest: it parses the
// committed role YAML and fails on an over-broad or unreviewed permission, so a
// change that widens the operator's access cannot merge without an explicit edit
// to the allow-list in this file.
package rbac

import (
	"bytes"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
)

// roleFiles are the two copies of the manager ClusterRole that ship to users:
// the kustomize install and the Helm chart. Both are generated (controller-gen /
// synced from it); this test treats them as read-only inputs. Paths are relative
// to this file's package dir (test/rbac), resolved up to the repo root.
var roleFiles = map[string]string{
	"kustomize (config/rbac/role.yaml)":                     "config/rbac/role.yaml",
	"helm (charts/decision-model-operator/files/role.yaml)": "charts/decision-model-operator/files/role.yaml",
}

// allowedRules is the committed allow-list of RBAC rules the operator may hold.
// It is the whole permitted rule set in canonical form (see canonicalRules): any
// rule in a role file that is not here fails the test, and any rule here that is
// missing from a role file fails too. Widening the operator's RBAC therefore
// requires an explicit, reviewable edit of this list — the point of item 69.
//
// Each entry is "apiGroups|resources|verbs", each field a comma-separated sorted
// list. The empty core group is written as "" (so a leading "|").
var allowedRules = []string{
	// core group
	`|configmaps|create,delete,get`,
	`|persistentvolumeclaims,services|create,delete,get,list,patch,update,watch`,
	`|pods|get,list,watch`,
	`|pods/status|get,patch,update`,
	`|secrets|get`,
	`,events.k8s.io|events|create,patch`,
	// apps
	`apps|deployments|create,delete,get,list,patch,update,watch`,
	`apps|replicasets|get,list,watch`,
	// batch
	`batch|jobs|create,delete,get,list,patch,update,watch`,
	// decisionmodel.io
	`decisionmodel.io|decisionmodels|create,delete,get,list,patch,update,watch`,
	`decisionmodel.io|decisionmodels/finalizers|update`,
	`decisionmodel.io|decisionmodels/status|get,patch,update`,
	// policy
	`policy|poddisruptionbudgets|create,delete,get,list,patch,update,watch`,
}

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	// This test lives in <root>/test/rbac; climb two levels.
	return filepath.Dir(filepath.Dir(wd))
}

func loadRole(t *testing.T, path string) rbacv1.ClusterRole {
	t.Helper()
	// #nosec G304 -- path is a constant from roleFiles, not user input.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var cr rbacv1.ClusterRole
	dec := k8syaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	if err := dec.Decode(&cr); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	if cr.Kind != "ClusterRole" {
		t.Fatalf("%s: expected kind ClusterRole, got %q", path, cr.Kind)
	}
	return cr
}

// canonicalRules renders a rule set as a sorted slice of "apiGroups|resources|verbs"
// strings, each field a comma-separated sorted list. Order within and between
// rules is normalised so the comparison does not depend on YAML ordering.
func canonicalRules(rules []rbacv1.PolicyRule) []string {
	out := make([]string, 0, len(rules))
	for _, r := range rules {
		groups := append([]string(nil), r.APIGroups...)
		resources := append([]string(nil), r.Resources...)
		verbs := append([]string(nil), r.Verbs...)
		sort.Strings(groups)
		sort.Strings(resources)
		sort.Strings(verbs)
		out = append(out,
			strings.Join(groups, ",")+"|"+strings.Join(resources, ",")+"|"+strings.Join(verbs, ","))
	}
	sort.Strings(out)
	return out
}

func contains(set []string, v string) bool {
	for _, s := range set {
		if s == v {
			return true
		}
	}
	return false
}

// TestNoWildcards fails on any "*" apiGroup, resource or verb: the operator must
// name exactly what it touches.
func TestNoWildcards(t *testing.T) {
	root := repoRoot(t)
	for name, rel := range roleFiles {
		cr := loadRole(t, filepath.Join(root, rel))
		for i, r := range cr.Rules {
			for _, g := range r.APIGroups {
				if g == "*" {
					t.Errorf("%s rule %d: wildcard apiGroup '*' is not allowed", name, i)
				}
			}
			for _, res := range r.Resources {
				if res == "*" {
					t.Errorf("%s rule %d: wildcard resource '*' is not allowed", name, i)
				}
			}
			for _, v := range r.Verbs {
				if v == "*" {
					t.Errorf("%s rule %d: wildcard verb '*' is not allowed (resources %v)", name, i, r.Resources)
				}
			}
		}
	}
}

// TestSecretsGetOnly fails if any rule grants a verb other than get on secrets:
// the operator reads referenced Secrets through the uncached API reader and must
// never list/watch/create/update/delete them.
func TestSecretsGetOnly(t *testing.T) {
	root := repoRoot(t)
	for name, rel := range roleFiles {
		cr := loadRole(t, filepath.Join(root, rel))
		for i, r := range cr.Rules {
			if !coreGroup(r.APIGroups) || !contains(r.Resources, "secrets") {
				continue
			}
			for _, v := range r.Verbs {
				if v != "get" {
					t.Errorf("%s rule %d: secrets may only be 'get', found %q", name, i, v)
				}
			}
		}
	}
}

// TestConfigMapsNoListWatch fails if any rule grants list or watch on configmaps:
// the operator reads its owned manifest ConfigMaps by name (get/create/delete) and
// must not run an informer or list over ConfigMaps.
func TestConfigMapsNoListWatch(t *testing.T) {
	root := repoRoot(t)
	for name, rel := range roleFiles {
		cr := loadRole(t, filepath.Join(root, rel))
		for i, r := range cr.Rules {
			if !coreGroup(r.APIGroups) || !contains(r.Resources, "configmaps") {
				continue
			}
			for _, v := range r.Verbs {
				if v == "list" || v == "watch" {
					t.Errorf("%s rule %d: configmaps must not have %q", name, i, v)
				}
			}
		}
	}
}

// TestRulesMatchAllowList fails on any rule not in the committed allow-list, and
// on any allow-list rule missing from a role file. Widening (or narrowing) the
// operator's RBAC must be a deliberate edit of allowedRules in this file.
func TestRulesMatchAllowList(t *testing.T) {
	root := repoRoot(t)
	want := append([]string(nil), allowedRules...)
	sort.Strings(want)
	for name, rel := range roleFiles {
		cr := loadRole(t, filepath.Join(root, rel))
		got := canonicalRules(cr.Rules)
		for _, g := range got {
			if !contains(want, g) {
				t.Errorf("%s: rule not in the allow-list (add it to allowedRules only after a deliberate audit): %s", name, g)
			}
		}
		for _, w := range want {
			if !contains(got, w) {
				t.Errorf("%s: allow-list rule is missing from the role (did RBAC shrink?): %s", name, w)
			}
		}
	}
}

// TestRoleFilesInSync fails if the kustomize and Helm copies of the ClusterRole
// diverge: they must grant exactly the same permissions.
func TestRoleFilesInSync(t *testing.T) {
	root := repoRoot(t)
	var first []string
	var firstName string
	for name, rel := range roleFiles {
		got := canonicalRules(loadRole(t, filepath.Join(root, rel)).Rules)
		if first == nil {
			first, firstName = got, name
			continue
		}
		if strings.Join(got, "\n") != strings.Join(first, "\n") {
			t.Errorf("role files out of sync:\n%s:\n  %s\n%s:\n  %s",
				firstName, strings.Join(first, "\n  "), name, strings.Join(got, "\n  "))
		}
	}
}

// coreGroup reports whether apiGroups denotes the core ("") group.
func coreGroup(groups []string) bool {
	for _, g := range groups {
		if g == "" {
			return true
		}
	}
	return false
}
