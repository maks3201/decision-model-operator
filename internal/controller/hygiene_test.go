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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
)

// reader() errors when APIReader is unset (no silent cached fallback).
func TestReaderRequiresAPIReader(t *testing.T) {
	r := &DecisionModelReconciler{}
	if _, err := r.reader(); err == nil {
		t.Fatal("expected reader() to error when APIReader is nil")
	}
	r.APIReader = noopReader{}
	if _, err := r.reader(); err != nil {
		t.Fatalf("expected reader() to succeed with APIReader set: %v", err)
	}
}

// noopReader is a minimal client.Reader stand-in for the reader() presence test.
type noopReader struct{}

func (noopReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return nil
}
func (noopReader) List(context.Context, client.ObjectList, ...client.ListOption) error { return nil }

// the Pod predicate ignores our own gate patch (a model-ready condition
// change) but enqueues on PodIP / ContainersReady / restartCount / deletion.
func TestPodPredicate(t *testing.T) {
	pred := podEnqueuePredicate()

	base := func() *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "p", ResourceVersion: "1"},
			Status: corev1.PodStatus{
				PodIP: "10.0.0.1",
				Conditions: []corev1.PodCondition{
					{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
				},
				ContainerStatuses: []corev1.ContainerStatus{{Name: "ollaya", RestartCount: 0}},
			},
		}
	}

	// Only the model-ready gate condition changed (our own patch) -> no enqueue.
	oldPod := base()
	newPod := base()
	newPod.ResourceVersion = "2"
	newPod.Status.Conditions = append(newPod.Status.Conditions, corev1.PodCondition{
		Type:   corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate),
		Status: corev1.ConditionTrue,
	})
	if pred.Update(event.UpdateEvent{ObjectOld: oldPod, ObjectNew: newPod}) {
		t.Error("gate-only change should NOT enqueue")
	}

	// PodIP change -> enqueue.
	oldPod, newPod = base(), base()
	newPod.Status.PodIP = "10.0.0.2"
	if !pred.Update(event.UpdateEvent{ObjectOld: oldPod, ObjectNew: newPod}) {
		t.Error("PodIP change should enqueue")
	}

	// ContainersReady flip -> enqueue.
	oldPod, newPod = base(), base()
	newPod.Status.Conditions[0].Status = corev1.ConditionFalse
	if !pred.Update(event.UpdateEvent{ObjectOld: oldPod, ObjectNew: newPod}) {
		t.Error("ContainersReady change should enqueue")
	}

	// restartCount change -> enqueue.
	oldPod, newPod = base(), base()
	newPod.Status.ContainerStatuses[0].RestartCount = 1
	if !pred.Update(event.UpdateEvent{ObjectOld: oldPod, ObjectNew: newPod}) {
		t.Error("restartCount change should enqueue")
	}

	// deletionTimestamp set -> enqueue.
	oldPod, newPod = base(), base()
	now := metav1.Now()
	newPod.DeletionTimestamp = &now
	if !pred.Update(event.UpdateEvent{ObjectOld: oldPod, ObjectNew: newPod}) {
		t.Error("deletionTimestamp change should enqueue")
	}

	// Create / Delete always enqueue.
	if !pred.Create(event.CreateEvent{Object: base()}) {
		t.Error("create should enqueue")
	}
	if !pred.Delete(event.DeleteEvent{Object: base()}) {
		t.Error("delete should enqueue")
	}
}
