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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
)

// CEL validation of spec.rollout.promotion.
var _ = Describe("promotion policy CEL", func() {
	var (
		ctx       context.Context
		namespace string
		counter   int
	)

	int32Ptr := func(v int32) *int32 { return &v }

	evalSpec := func() *decisionmodelv1alpha1.EvaluationSpec {
		return &decisionmodelv1alpha1.EvaluationSpec{
			DatasetRef: decisionmodelv1alpha1.DatasetRef{
				ConfigMapRef: &decisionmodelv1alpha1.DatasetKeyRef{Name: "golden", Key: "cases.jsonl"},
			},
			MinAccuracy: "0.90",
		}
	}

	mk := func(name string, rollout *decisionmodelv1alpha1.RolloutSpec) *decisionmodelv1alpha1.DecisionModel {
		return &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1), Rollout: rollout,
			},
		}
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		namespace = fmt.Sprintf("promo-cel-%d", counter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	It("rejects EvaluationGated without evaluation and accepts it with evaluation", func() {
		err := k8sClient.Create(ctx, mk("eg-bad", &decisionmodelv1alpha1.RolloutSpec{
			Promotion: decisionmodelv1alpha1.PromotionEvaluationGated,
		}))
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "want Invalid, got %v", err)

		Expect(k8sClient.Create(ctx, mk("eg-good", &decisionmodelv1alpha1.RolloutSpec{
			Promotion: decisionmodelv1alpha1.PromotionEvaluationGated, Evaluation: evalSpec(),
		}))).To(Succeed())
	})

	It("rejects manualPromotion:true alongside a conflicting promotion, accepts promotion: Manual", func() {
		//nolint:staticcheck // exercising the deprecated alias conflict rule
		err := k8sClient.Create(ctx, mk("conflict", &decisionmodelv1alpha1.RolloutSpec{
			Promotion: decisionmodelv1alpha1.PromotionAutomatic, ManualPromotion: true,
		}))
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "want Invalid, got %v", err)

		Expect(k8sClient.Create(ctx, mk("manual", &decisionmodelv1alpha1.RolloutSpec{
			Promotion: decisionmodelv1alpha1.PromotionManual,
		}))).To(Succeed())
	})

	It("accepts manualPromotion:true as a standalone alias", func() {
		//nolint:staticcheck // exercising the deprecated alias
		Expect(k8sClient.Create(ctx, mk("alias", &decisionmodelv1alpha1.RolloutSpec{
			ManualPromotion: true,
		}))).To(Succeed())
	})

	It("accepts Automatic with and without evaluation", func() {
		Expect(k8sClient.Create(ctx, mk("auto-bare", &decisionmodelv1alpha1.RolloutSpec{
			Promotion: decisionmodelv1alpha1.PromotionAutomatic,
		}))).To(Succeed())
		Expect(k8sClient.Create(ctx, mk("auto-eval", &decisionmodelv1alpha1.RolloutSpec{
			Promotion: decisionmodelv1alpha1.PromotionAutomatic, Evaluation: evalSpec(),
		}))).To(Succeed())
	})
})
