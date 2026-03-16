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

type AetherMQConsumerGroupReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

func (r *AetherMQConsumerGroupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	group := &aethermqv1.AetherMQConsumerGroup{}
	if err := r.Get(ctx, req.NamespacedName, group); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if group.DeletionTimestamp != nil {
		logger.Info("ConsumerGroup deleted", "name", group.Name)
		return ctrl.Result{}, nil
	}

	logger.Info("Reconciling consumer group", "name", group.Name, "groupName", group.Spec.GroupName)

	group.Status.Phase = "Ready"
	if err := r.Status().Update(ctx, group); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *AetherMQConsumerGroupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aethermqv1.AetherMQConsumerGroup{}).
		Complete(r)
}
