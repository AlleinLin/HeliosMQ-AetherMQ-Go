package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func (in *ResourceList) DeepCopyInto(out *ResourceList) {
	*out = make(ResourceList, len(*in))
	for key, val := range *in {
		(*out)[key] = val.DeepCopy()
	}
}

func (in *ResourceList) DeepCopy() *ResourceList {
	if in == nil {
		return nil
	}
	out := new(ResourceList)
	in.DeepCopyInto(out)
	return out
}

func (in *AetherMQClusterSpec) DeepCopyInto(out *AetherMQClusterSpec) {
	*out = *in
	in.Controller.DeepCopyInto(&out.Controller)
	in.Broker.DeepCopyInto(&out.Broker)
	in.Gateway.DeepCopyInto(&out.Gateway)
	out.Storage = in.Storage
	out.Config = in.Config
}

func (in *AetherMQClusterSpec) DeepCopy() *AetherMQClusterSpec {
	if in == nil {
		return nil
	}
	out := new(AetherMQClusterSpec)
	in.DeepCopyInto(out)
	return out
}

func (in *ControllerSpec) DeepCopyInto(out *ControllerSpec) {
	*out = *in
	if in.Replicas != nil {
		in, out := &in.Replicas, &out.Replicas
		*out = new(int32)
		**out = **in
	}
	in.Resources.DeepCopyInto(&out.Resources)
	if in.Affinity != nil {
		in, out := &in.Affinity, &out.Affinity
		*out = new(corev1.Affinity)
		(*in).DeepCopyInto(*out)
	}
	if in.Tolerations != nil {
		in, out := &in.Tolerations, &out.Tolerations
		*out = make([]corev1.Toleration, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	if in.NodeSelector != nil {
		in, out := &in.NodeSelector, &out.NodeSelector
		*out = make(map[string]string, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
}

func (in *ControllerSpec) DeepCopy() *ControllerSpec {
	if in == nil {
		return nil
	}
	out := new(ControllerSpec)
	in.DeepCopyInto(out)
	return out
}

func (in *BrokerSpec) DeepCopyInto(out *BrokerSpec) {
	*out = *in
	if in.Replicas != nil {
		in, out := &in.Replicas, &out.Replicas
		*out = new(int32)
		**out = **in
	}
	in.Resources.DeepCopyInto(&out.Resources)
	if in.Affinity != nil {
		in, out := &in.Affinity, &out.Affinity
		*out = new(corev1.Affinity)
		(*in).DeepCopyInto(*out)
	}
	if in.Tolerations != nil {
		in, out := &in.Tolerations, &out.Tolerations
		*out = make([]corev1.Toleration, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	if in.NodeSelector != nil {
		in, out := &in.NodeSelector, &out.NodeSelector
		*out = make(map[string]string, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
	out.Config = in.Config
}

func (in *BrokerSpec) DeepCopy() *BrokerSpec {
	if in == nil {
		return nil
	}
	out := new(BrokerSpec)
	in.DeepCopyInto(out)
	return out
}

func (in *GatewaySpec) DeepCopyInto(out *GatewaySpec) {
	*out = *in
	if in.Replicas != nil {
		in, out := &in.Replicas, &out.Replicas
		*out = new(int32)
		**out = **in
	}
	in.Resources.DeepCopyInto(&out.Resources)
	if in.Affinity != nil {
		in, out := &in.Affinity, &out.Affinity
		*out = new(corev1.Affinity)
		(*in).DeepCopyInto(*out)
	}
	if in.Tolerations != nil {
		in, out := &in.Tolerations, &out.Tolerations
		*out = make([]corev1.Toleration, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	if in.NodeSelector != nil {
		in, out := &in.NodeSelector, &out.NodeSelector
		*out = make(map[string]string, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
	out.Service = in.Service
}

func (in *GatewaySpec) DeepCopy() *GatewaySpec {
	if in == nil {
		return nil
	}
	out := new(GatewaySpec)
	in.DeepCopyInto(out)
	return out
}

func (in *StorageSpec) DeepCopyInto(out *StorageSpec) {
	*out = *in
}

func (in *StorageSpec) DeepCopy() *StorageSpec {
	if in == nil {
		return nil
	}
	out := new(StorageSpec)
	in.DeepCopyInto(out)
	return out
}

func (in *ConfigSpec) DeepCopyInto(out *ConfigSpec) {
	*out = *in
}

func (in *ConfigSpec) DeepCopy() *ConfigSpec {
	if in == nil {
		return nil
	}
	out := new(ConfigSpec)
	in.DeepCopyInto(out)
	return out
}

func (in *BrokerConfig) DeepCopyInto(out *BrokerConfig) {
	*out = *in
}

func (in *BrokerConfig) DeepCopy() *BrokerConfig {
	if in == nil {
		return nil
	}
	out := new(BrokerConfig)
	in.DeepCopyInto(out)
	return out
}

func (in *ServiceConfig) DeepCopyInto(out *ServiceConfig) {
	*out = *in
}

func (in *ServiceConfig) DeepCopy() *ServiceConfig {
	if in == nil {
		return nil
	}
	out := new(ServiceConfig)
	in.DeepCopyInto(out)
	return out
}

func (in *ResourceRequirements) DeepCopyInto(out *ResourceRequirements) {
	*out = *in
	if in.Requests != nil {
		in, out := &in.Requests, &out.Requests
		*out = make(ResourceList, len(*in))
		for key, val := range *in {
			(*out)[key] = val.DeepCopy()
		}
	}
	if in.Limits != nil {
		in, out := &in.Limits, &out.Limits
		*out = make(ResourceList, len(*in))
		for key, val := range *in {
			(*out)[key] = val.DeepCopy()
		}
	}
}

func (in *ResourceRequirements) DeepCopy() *ResourceRequirements {
	if in == nil {
		return nil
	}
	out := new(ResourceRequirements)
	in.DeepCopyInto(out)
	return out
}

func (in *AetherMQClusterStatus) DeepCopyInto(out *AetherMQClusterStatus) {
	*out = *in
	if in.Conditions != nil {
		in, out := &in.Conditions, &out.Conditions
		*out = make([]metav1.Condition, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	if in.ControllerStatus != nil {
		in, out := &in.ControllerStatus, &out.ControllerStatus
		*out = new(ComponentStatus)
		**out = **in
	}
	if in.BrokerStatus != nil {
		in, out := &in.BrokerStatus, &out.BrokerStatus
		*out = new(ComponentStatus)
		**out = **in
	}
	if in.GatewayStatus != nil {
		in, out := &in.GatewayStatus, &out.GatewayStatus
		*out = new(ComponentStatus)
		**out = **in
	}
}

func (in *AetherMQClusterStatus) DeepCopy() *AetherMQClusterStatus {
	if in == nil {
		return nil
	}
	out := new(AetherMQClusterStatus)
	in.DeepCopyInto(out)
	return out
}

func (in *ComponentStatus) DeepCopyInto(out *ComponentStatus) {
	*out = *in
}

func (in *ComponentStatus) DeepCopy() *ComponentStatus {
	if in == nil {
		return nil
	}
	out := new(ComponentStatus)
	in.DeepCopyInto(out)
	return out
}

func (in *AetherMQCluster) DeepCopyInto(out *AetherMQCluster) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

func (in *AetherMQCluster) DeepCopy() *AetherMQCluster {
	if in == nil {
		return nil
	}
	out := new(AetherMQCluster)
	in.DeepCopyInto(out)
	return out
}

func (in *AetherMQCluster) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *AetherMQClusterList) DeepCopyInto(out *AetherMQClusterList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]AetherMQCluster, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

func (in *AetherMQClusterList) DeepCopy() *AetherMQClusterList {
	if in == nil {
		return nil
	}
	out := new(AetherMQClusterList)
	in.DeepCopyInto(out)
	return out
}

func (in *AetherMQClusterList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *AetherMQTopicSpec) DeepCopyInto(out *AetherMQTopicSpec) {
	*out = *in
}

func (in *AetherMQTopicSpec) DeepCopy() *AetherMQTopicSpec {
	if in == nil {
		return nil
	}
	out := new(AetherMQTopicSpec)
	in.DeepCopyInto(out)
	return out
}

func (in *AetherMQTopicStatus) DeepCopyInto(out *AetherMQTopicStatus) {
	*out = *in
	if in.Conditions != nil {
		in, out := &in.Conditions, &out.Conditions
		*out = make([]metav1.Condition, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

func (in *AetherMQTopicStatus) DeepCopy() *AetherMQTopicStatus {
	if in == nil {
		return nil
	}
	out := new(AetherMQTopicStatus)
	in.DeepCopyInto(out)
	return out
}

func (in *AetherMQTopic) DeepCopyInto(out *AetherMQTopic) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = in.Spec
	in.Status.DeepCopyInto(&out.Status)
}

func (in *AetherMQTopic) DeepCopy() *AetherMQTopic {
	if in == nil {
		return nil
	}
	out := new(AetherMQTopic)
	in.DeepCopyInto(out)
	return out
}

func (in *AetherMQTopic) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *AetherMQTopicList) DeepCopyInto(out *AetherMQTopicList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]AetherMQTopic, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

func (in *AetherMQTopicList) DeepCopy() *AetherMQTopicList {
	if in == nil {
		return nil
	}
	out := new(AetherMQTopicList)
	in.DeepCopyInto(out)
	return out
}

func (in *AetherMQTopicList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *AetherMQConsumerGroupSpec) DeepCopyInto(out *AetherMQConsumerGroupSpec) {
	*out = *in
	if in.Topics != nil {
		in, out := &in.Topics, &out.Topics
		*out = make([]string, len(*in))
		copy(*out, *in)
	}
}

func (in *AetherMQConsumerGroupSpec) DeepCopy() *AetherMQConsumerGroupSpec {
	if in == nil {
		return nil
	}
	out := new(AetherMQConsumerGroupSpec)
	in.DeepCopyInto(out)
	return out
}

func (in *AetherMQConsumerGroupStatus) DeepCopyInto(out *AetherMQConsumerGroupStatus) {
	*out = *in
	if in.Conditions != nil {
		in, out := &in.Conditions, &out.Conditions
		*out = make([]metav1.Condition, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	in.LastRebalanceAt.DeepCopyInto(&out.LastRebalanceAt)
}

func (in *AetherMQConsumerGroupStatus) DeepCopy() *AetherMQConsumerGroupStatus {
	if in == nil {
		return nil
	}
	out := new(AetherMQConsumerGroupStatus)
	in.DeepCopyInto(out)
	return out
}

func (in *AetherMQConsumerGroup) DeepCopyInto(out *AetherMQConsumerGroup) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

func (in *AetherMQConsumerGroup) DeepCopy() *AetherMQConsumerGroup {
	if in == nil {
		return nil
	}
	out := new(AetherMQConsumerGroup)
	in.DeepCopyInto(out)
	return out
}

func (in *AetherMQConsumerGroup) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *AetherMQConsumerGroupList) DeepCopyInto(out *AetherMQConsumerGroupList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]AetherMQConsumerGroup, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

func (in *AetherMQConsumerGroupList) DeepCopy() *AetherMQConsumerGroupList {
	if in == nil {
		return nil
	}
	out := new(AetherMQConsumerGroupList)
	in.DeepCopyInto(out)
	return out
}

func (in *AetherMQConsumerGroupList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}
