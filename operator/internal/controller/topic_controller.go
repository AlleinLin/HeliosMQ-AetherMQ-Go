package controller

import (
	"context"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	aethermqv1 "github.com/aethermq/aethermq-operator/api/v1"
)

type AetherMQTopicReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

func (r *AetherMQTopicReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	topic := &aethermqv1.AetherMQTopic{}
	if err := r.Get(ctx, req.NamespacedName, topic); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if topic.DeletionTimestamp != nil {
		logger.Info("Topic deleted", "name", topic.Name)
		return ctrl.Result{}, nil
	}

	logger.Info("Reconciling topic", "name", topic.Name, "topicName", topic.Spec.TopicName)

	topic.Status.Phase = "Ready"
	if err := r.Status().Update(ctx, topic); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *AetherMQTopicReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aethermqv1.AetherMQTopic{}).
		Complete(r)
}
