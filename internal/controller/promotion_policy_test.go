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
	"testing"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
)

// TestEffectivePromotionPolicy pins the documented defaults and the deprecated
// manualPromotion alias: an explicit promotion wins; otherwise manualPromotion
// means Manual; otherwise EvaluationGated when evaluation is set, else Automatic.
func TestEffectivePromotionPolicy(t *testing.T) {
	evalSpec := &decisionmodelv1alpha1.EvaluationSpec{MinAccuracy: "0.9"}

	tests := []struct {
		name    string
		rollout *decisionmodelv1alpha1.RolloutSpec
		want    decisionmodelv1alpha1.PromotionPolicy
	}{
		{"no rollout", nil, decisionmodelv1alpha1.PromotionAutomatic},
		{"empty rollout", &decisionmodelv1alpha1.RolloutSpec{}, decisionmodelv1alpha1.PromotionAutomatic},
		{
			"evaluation set defaults to EvaluationGated",
			&decisionmodelv1alpha1.RolloutSpec{Evaluation: evalSpec},
			decisionmodelv1alpha1.PromotionEvaluationGated,
		},
		{
			"deprecated manualPromotion alias",
			//nolint:staticcheck // exercising the deprecated alias on purpose
			&decisionmodelv1alpha1.RolloutSpec{ManualPromotion: true},
			decisionmodelv1alpha1.PromotionManual,
		},
		{
			"explicit Automatic overrides evaluation default",
			&decisionmodelv1alpha1.RolloutSpec{Promotion: decisionmodelv1alpha1.PromotionAutomatic, Evaluation: evalSpec},
			decisionmodelv1alpha1.PromotionAutomatic,
		},
		{
			"explicit Manual",
			&decisionmodelv1alpha1.RolloutSpec{Promotion: decisionmodelv1alpha1.PromotionManual},
			decisionmodelv1alpha1.PromotionManual,
		},
		{
			"explicit promotion wins over alias",
			//nolint:staticcheck // alias set alongside explicit promotion
			&decisionmodelv1alpha1.RolloutSpec{Promotion: decisionmodelv1alpha1.PromotionManual, ManualPromotion: true},
			decisionmodelv1alpha1.PromotionManual,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dm := &decisionmodelv1alpha1.DecisionModel{}
			dm.Spec.Rollout = tc.rollout
			if got := effectivePromotionPolicy(dm); got != tc.want {
				t.Errorf("effectivePromotionPolicy = %q, want %q", got, tc.want)
			}
			wantManual := tc.want == decisionmodelv1alpha1.PromotionManual
			if got := manualPromotion(dm); got != wantManual {
				t.Errorf("manualPromotion = %v, want %v", got, wantManual)
			}
		})
	}
}
