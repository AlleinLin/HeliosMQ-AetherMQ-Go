package v1

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

var aethermqclusterlog = logf.Log.WithName("aethermqcluster-resource")

//+kubebuilder:webhook:path=/mutate-aethermq-io-v1-aethermqcluster,mutating=true,failurePolicy=fail,sideEffects=None,groups=aethermq.io,resources=aethermqclusters,verbs=create;update,versions=v1,name=maethermqcluster.kb.io,admissionReviewVersions=v1

var _ webhook.Defaulter = &AetherMQCluster{}

func (r *AetherMQCluster) Default() {
	aethermqclusterlog.Info("default", "name", r.Name)

	if r.Spec.Controller.Replicas == nil {
		r.Spec.Controller.Replicas = int32Ptr(3)
	}
	if r.Spec.Broker.Replicas == nil {
		r.Spec.Broker.Replicas = int32Ptr(3)
	}
	if r.Spec.Gateway.Replicas == nil {
		r.Spec.Gateway.Replicas = int32Ptr(2)
	}

	if r.Spec.Broker.Resources.Requests == nil {
		r.Spec.Broker.Resources.Requests = make(ResourceList)
	}
	if r.Spec.Broker.Resources.Limits == nil {
		r.Spec.Broker.Resources.Limits = make(ResourceList)
	}

	if r.Spec.Storage.Size == "" {
		r.Spec.Storage.Size = "100Gi"
	}
	if r.Spec.Storage.StorageClass == "" {
		r.Spec.Storage.StorageClass = "standard"
	}
}

//+kubebuilder:webhook:path=/validate-aethermq-io-v1-aethermqcluster,mutating=false,failurePolicy=fail,sideEffects=None,groups=aethermq.io,resources=aethermqclusters,verbs=create;update,versions=v1,name=vaethermqcluster.kb.io,admissionReviewVersions=v1

var _ webhook.Validator = &AetherMQCluster{}

func (r *AetherMQCluster) ValidateCreate() (admission.Warnings, error) {
	aethermqclusterlog.Info("validate create", "name", r.Name)

	if *r.Spec.Controller.Replicas < 1 {
		return nil, fmt.Errorf("controller replicas must be at least 1")
	}
	if *r.Spec.Controller.Replicas%2 == 0 {
		return nil, fmt.Errorf("controller replicas must be odd for Raft consensus")
	}
	if *r.Spec.Broker.Replicas < 1 {
		return nil, fmt.Errorf("broker replicas must be at least 1")
	}

	return nil, nil
}

func (r *AetherMQCluster) ValidateUpdate(old runtime.Object) (admission.Warnings, error) {
	aethermqclusterlog.Info("validate update", "name", r.Name)

	oldCluster := old.(*AetherMQCluster)

	if *r.Spec.Controller.Replicas < *oldCluster.Spec.Controller.Replicas {
		return nil, fmt.Errorf("controller scale down is not supported")
	}

	return r.ValidateCreate()
}

func (r *AetherMQCluster) ValidateDelete() (admission.Warnings, error) {
	aethermqclusterlog.Info("validate delete", "name", r.Name)
	return nil, nil
}

func int32Ptr(i int32) *int32 {
	return &i
}
