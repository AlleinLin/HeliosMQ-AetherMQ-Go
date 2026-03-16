package v1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type ResourceList map[corev1.ResourceName]resource.Quantity

type AetherMQClusterSpec struct {
	Controller ControllerSpec `json:"controller"`
	Broker     BrokerSpec     `json:"broker"`
	Gateway    GatewaySpec    `json:"gateway"`
	Storage    StorageSpec    `json:"storage"`
	Config     ConfigSpec     `json:"config,omitempty"`
}

type ControllerSpec struct {
	Replicas     *int32               `json:"replicas"`
	Resources    ResourceRequirements `json:"resources,omitempty"`
	Affinity     *corev1.Affinity     `json:"affinity,omitempty"`
	Tolerations  []corev1.Toleration  `json:"tolerations,omitempty"`
	NodeSelector map[string]string    `json:"nodeSelector,omitempty"`
}

type BrokerSpec struct {
	Replicas     *int32               `json:"replicas"`
	Resources    ResourceRequirements `json:"resources,omitempty"`
	Affinity     *corev1.Affinity     `json:"affinity,omitempty"`
	Tolerations  []corev1.Toleration  `json:"tolerations,omitempty"`
	NodeSelector map[string]string    `json:"nodeSelector,omitempty"`
	Config       BrokerConfig         `json:"config,omitempty"`
}

type GatewaySpec struct {
	Replicas     *int32               `json:"replicas"`
	Resources    ResourceRequirements `json:"resources,omitempty"`
	Affinity     *corev1.Affinity     `json:"affinity,omitempty"`
	Tolerations  []corev1.Toleration  `json:"tolerations,omitempty"`
	NodeSelector map[string]string    `json:"nodeSelector,omitempty"`
	Service      ServiceConfig        `json:"service,omitempty"`
}

type StorageSpec struct {
	Size         string `json:"size"`
	StorageClass string `json:"storageClass"`
	AccessMode   string `json:"accessMode,omitempty"`
}

type ConfigSpec struct {
	DefaultTopicPartitions       int32 `json:"defaultTopicPartitions,omitempty"`
	DefaultReplicationFactor     int32 `json:"defaultReplicationFactor,omitempty"`
	DefaultRetentionMs           int64 `json:"defaultRetentionMs,omitempty"`
	MaxMessageSize               int64 `json:"maxMessageSize,omitempty"`
	EnableAutoTopicCreation      bool  `json:"enableAutoTopicCreation,omitempty"`
	EnableAutoPartitionRebalance bool  `json:"enableAutoPartitionRebalance,omitempty"`
}

type BrokerConfig struct {
	SegmentSizeMB   int64  `json:"segmentSizeMB,omitempty"`
	FlushIntervalMs int64  `json:"flushIntervalMs,omitempty"`
	MaxConnections  int32  `json:"maxConnections,omitempty"`
	CompressionType string `json:"compressionType,omitempty"`
}

type ServiceConfig struct {
	Type     corev1.ServiceType `json:"type,omitempty"`
	Port     int32              `json:"port,omitempty"`
	NodePort int32              `json:"nodePort,omitempty"`
}

type ResourceRequirements struct {
	Requests ResourceList `json:"requests,omitempty"`
	Limits   ResourceList `json:"limits,omitempty"`
}

type AetherMQClusterStatus struct {
	Phase               string             `json:"phase,omitempty"`
	Conditions          []metav1.Condition `json:"conditions,omitempty"`
	ControllerStatus    *ComponentStatus   `json:"controllerStatus,omitempty"`
	BrokerStatus        *ComponentStatus   `json:"brokerStatus,omitempty"`
	GatewayStatus       *ComponentStatus   `json:"gatewayStatus,omitempty"`
	TotalTopics         int32              `json:"totalTopics,omitempty"`
	TotalPartitions     int32              `json:"totalPartitions,omitempty"`
	TotalConsumerGroups int32              `json:"totalConsumerGroups,omitempty"`
	MessagesPerSecond   int64              `json:"messagesPerSecond,omitempty"`
}

type ComponentStatus struct {
	Replicas      int32  `json:"replicas"`
	ReadyReplicas int32  `json:"readyReplicas"`
	Version       string `json:"version,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

type AetherMQCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AetherMQClusterSpec   `json:"spec,omitempty"`
	Status AetherMQClusterStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

type AetherMQClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AetherMQCluster `json:"items"`
}

type AetherMQTopicSpec struct {
	TopicName         string `json:"topicName"`
	Partitions        int32  `json:"partitions"`
	ReplicationFactor int32  `json:"replicationFactor"`
	RetentionMs       int64  `json:"retentionMs,omitempty"`
	RetentionBytes    int64  `json:"retentionBytes,omitempty"`
	SegmentMs         int64  `json:"segmentMs,omitempty"`
	SegmentBytes      int64  `json:"segmentBytes,omitempty"`
	CompressionType   string `json:"compressionType,omitempty"`
	CleanupPolicy     string `json:"cleanupPolicy,omitempty"`
	MinInSyncReplicas int32  `json:"minInSyncReplicas,omitempty"`
	EnableDelay       bool   `json:"enableDelay,omitempty"`
	MaxRetry          int32  `json:"maxRetry,omitempty"`
	DLQEnabled        bool   `json:"dlqEnabled,omitempty"`
}

type AetherMQTopicStatus struct {
	Phase          string             `json:"phase,omitempty"`
	Conditions     []metav1.Condition `json:"conditions,omitempty"`
	PartitionCount int32              `json:"partitionCount,omitempty"`
	TotalMessages  int64              `json:"totalMessages,omitempty"`
	TotalBytes     int64              `json:"totalBytes,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

type AetherMQTopic struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AetherMQTopicSpec   `json:"spec,omitempty"`
	Status AetherMQTopicStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

type AetherMQTopicList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AetherMQTopic `json:"items"`
}

type AetherMQConsumerGroupSpec struct {
	GroupName        string   `json:"groupName"`
	Topics           []string `json:"topics"`
	SubscriptionType string   `json:"subscriptionType,omitempty"`
	AckMode          string   `json:"ackMode,omitempty"`
	MaxRetry         int32    `json:"maxRetry,omitempty"`
	DLQEnabled       bool     `json:"dlqEnabled,omitempty"`
	AutoOffsetReset  string   `json:"autoOffsetReset,omitempty"`
}

type AetherMQConsumerGroupStatus struct {
	Phase           string             `json:"phase,omitempty"`
	Conditions      []metav1.Condition `json:"conditions,omitempty"`
	MemberCount     int32              `json:"memberCount,omitempty"`
	TotalLag        int64              `json:"totalLag,omitempty"`
	LastRebalanceAt metav1.Time        `json:"lastRebalanceAt,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

type AetherMQConsumerGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AetherMQConsumerGroupSpec   `json:"spec,omitempty"`
	Status AetherMQConsumerGroupStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

type AetherMQConsumerGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AetherMQConsumerGroup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AetherMQCluster{}, &AetherMQClusterList{})
	SchemeBuilder.Register(&AetherMQTopic{}, &AetherMQTopicList{})
	SchemeBuilder.Register(&AetherMQConsumerGroup{}, &AetherMQConsumerGroupList{})
}
