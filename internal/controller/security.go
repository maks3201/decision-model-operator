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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// canonicalNamer is the optional engine capability used to validate model names
// (also used by the prober).
type canonicalNamer interface {
	CanonicalName(name string) (string, error)
}

// validateModelName returns an error if the engine considers spec.model invalid.
// Engines without CanonicalName are not validated here.
func validateModelName(eng engine.Engine, model string) error {
	if c, ok := eng.(canonicalNamer); ok {
		if _, err := c.CanonicalName(model); err != nil {
			return err
		}
	}
	return nil
}

// allowedRegistries returns the configured allow-list or the default.
func (r *DecisionModelReconciler) allowedRegistries() []string {
	if len(r.AllowedRegistries) == 0 {
		return []string{defaultAllowedRegistry}
	}
	return r.AllowedRegistries
}

// checkRegistryAllowed validates the model's registry host against the
// allow-list and insecure policy, using the engine's own parser via the
// engine.RegistryHoster contract. It fails closed: an engine without the
// capability may only use its default registry, and a parse error is treated as
// an invalid model name. Returns a reason ("" when allowed) and a message.
func (r *DecisionModelReconciler) checkRegistryAllowed(eng engine.Engine, model string) (reason, message string) {
	rh, ok := eng.(engine.RegistryHoster)
	if !ok {
		// Fail closed: without the capability the operator cannot know which host
		// the engine targets, so it must deny.
		return reasonRegistryNotAllowed,
			fmt.Sprintf("engine %q cannot report registry hosts", eng.Name())
	}
	host, insecure, err := rh.RegistryHost(model)
	if err != nil {
		return reasonInvalidModelName, err.Error()
	}
	hostBare := strings.ToLower(strings.TrimSpace(host))
	allowed := false
	for _, a := range r.allowedRegistries() {
		if a == hostBare { // allow-list is pre-normalised (trim+lower) at startup
			allowed = true
			break
		}
	}
	if !allowed {
		return reasonRegistryNotAllowed,
			fmt.Sprintf("registry %q is not in the allowed list %v", hostBare, r.allowedRegistries())
	}
	if insecure && !r.AllowInsecureRegistries {
		return reasonRegistryNotAllowed,
			fmt.Sprintf("insecure http registry %q not allowed (set --allow-insecure-registries)", hostBare)
	}
	return "", ""
}

// guardSecurity enforces the model-name, registry allow-list and
// image-override policies. On a violation it sets phase Failed with a
// Ready=False condition, persists status, and returns done=true with a
// reconcile.TerminalError (these are permanent — retrying the same spec cannot
// help). It never creates workloads.
func (r *DecisionModelReconciler) guardSecurity(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
) (ctrl.Result, error, bool) {
	fail := func(reason, message string) (ctrl.Result, error, bool) {
		setStatusCondition(dm, metav1.Condition{
			Type:    decisionmodelv1alpha1.ConditionReady,
			Status:  metav1.ConditionFalse,
			Reason:  reason,
			Message: message,
		})
		r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseFailed)
		res, err := r.finish(ctx, dm, ctrl.Result{}, nil)
		if err != nil {
			// Status write failed (e.g. conflict) — let the caller requeue.
			return res, err, true
		}
		return res, reconcile.TerminalError(fmt.Errorf("%s: %s", reason, message)), true
	}

	// model name must be valid (also when spec.digest is set).
	if err := validateModelName(eng, dm.Spec.Model); err != nil {
		return fail(reasonInvalidModelName, err.Error())
	}
	// registry host must be allowed.
	if reason, msg := r.checkRegistryAllowed(eng, dm.Spec.Model); reason != "" {
		return fail(reason, msg)
	}
	// spec.image override must be permitted.
	if dm.Spec.Image != "" && !r.AllowImageOverride {
		return fail(reasonImageOverrideNotAllowed,
			"spec.image override is not allowed (set --allow-image-override)")
	}
	return ctrl.Result{}, nil, false
}

// apiKey reads the engine API key from the referenced Secret, if any.
func (r *DecisionModelReconciler) apiKey(ctx context.Context, dm *decisionmodelv1alpha1.DecisionModel) (string, error) {
	if dm.Spec.Auth == nil || dm.Spec.Auth.APIKeySecretRef == nil {
		return "", nil
	}
	ref := dm.Spec.Auth.APIKeySecretRef
	rdr, err := r.reader()
	if err != nil {
		return "", err
	}
	var secret corev1.Secret
	if err := rdr.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: ref.Name}, &secret); err != nil {
		return "", err
	}
	if err := requireAPIKeyLabel(secret.Labels); err != nil {
		return "", err
	}
	// The referenced key must exist: optional is rejected by CEL, and a labelled
	// Secret missing the key would leave the prober and the serving Pod reading
	// different keys (empty vs real). Surface it instead of silently using "".
	if _, ok := secret.Data[ref.Key]; !ok {
		return "", fmt.Errorf("%w: Secret %s/%s has no key %q",
			errAPIKeyInvalid, dm.Namespace, ref.Name, ref.Key)
	}
	return string(secret.Data[ref.Key]), nil
}

// validateDownloadToken checks the download-token Secret referenced by
// spec.cache.downloadTokenSecretRef (if any): it must exist, carry the
// decisionmodel.io/download-token=true opt-in label, and contain the referenced
// key. optional is not supported: the engine renders optional=false on the Job
// env var, so a missing Secret always fails the Job. Returns
// errSecretNotAllowed on a missing label and errDownloadTokenInvalid on a
// missing key. The token value is not read here: the engine injects it into the
// prefetch Job from the Secret ref.
func (r *DecisionModelReconciler) validateDownloadToken(
	ctx context.Context, dm *decisionmodelv1alpha1.DecisionModel,
) error {
	if dm.Spec.Cache == nil || dm.Spec.Cache.DownloadTokenSecretRef == nil {
		return nil
	}
	ref := dm.Spec.Cache.DownloadTokenSecretRef
	rdr, err := r.reader()
	if err != nil {
		return err
	}
	var secret corev1.Secret
	if err := rdr.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: ref.Name}, &secret); err != nil {
		// optional is not supported; treat a missing Secret the same as a missing
		// label so the operator never creates a Job that cannot start.
		return err
	}
	if err := requireDownloadTokenLabel(secret.Labels); err != nil {
		return err
	}
	if _, ok := secret.Data[ref.Key]; !ok {
		return fmt.Errorf("%w: Secret %s/%s has no key %q",
			errDownloadTokenInvalid, dm.Namespace, ref.Name, ref.Key)
	}
	return nil
}

// reader returns the uncached APIReader for Secrets/ConfigMaps. It is a hard
// error to use it unset: the manager must inject mgr.GetAPIReader() so the
// operator never starts cluster-wide Secret/ConfigMap informers.
func (r *DecisionModelReconciler) reader() (client.Reader, error) {
	if r.APIReader == nil {
		return nil, fmt.Errorf("APIReader is not configured")
	}
	return r.APIReader, nil
}
