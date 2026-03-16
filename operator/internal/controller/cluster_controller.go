package controller

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	aethermqv1 "github.com/aethermq/aethermq-operator/api/v1"
)

const (
	clusterFinalizer = "aethermq.io/finalizer"
	appLabel         = "app.kubernetes.io/name"
	componentLabel   = "app.kubernetes.io/component"
	instanceLabel    = "app.kubernetes.io/instance"
	versionLabel     = "app.kubernetes.io/version"
)

type AetherMQClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

func (r *AetherMQClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	cluster := &aethermqv1.AetherMQCluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if cluster.DeletionTimestamp != nil {
		if controllerutil.ContainsFinalizer(cluster, clusterFinalizer) {
			if err := r.cleanupCluster(ctx, cluster); err != nil {
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(cluster, clusterFinalizer)
			if err := r.Update(ctx, cluster); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(cluster, clusterFinalizer) {
		controllerutil.AddFinalizer(cluster, clusterFinalizer)
		if err := r.Update(ctx, cluster); err != nil {
			return ctrl.Result{}, err
		}
	}

	if err := r.reconcileController(ctx, cluster); err != nil {
		logger.Error(err, "Failed to reconcile controller")
		return ctrl.Result{}, err
	}

	if err := r.reconcileBroker(ctx, cluster); err != nil {
		logger.Error(err, "Failed to reconcile broker")
		return ctrl.Result{}, err
	}

	if err := r.reconcileGateway(ctx, cluster); err != nil {
		logger.Error(err, "Failed to reconcile gateway")
		return ctrl.Result{}, err
	}

	if err := r.reconcileServices(ctx, cluster); err != nil {
		logger.Error(err, "Failed to reconcile services")
		return ctrl.Result{}, err
	}

	if err := r.reconcileConfigMaps(ctx, cluster); err != nil {
		logger.Error(err, "Failed to reconcile configmaps")
		return ctrl.Result{}, err
	}

	if err := r.updateStatus(ctx, cluster); err != nil {
		logger.Error(err, "Failed to update status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *AetherMQClusterReconciler) reconcileController(ctx context.Context, cluster *aethermqv1.AetherMQCluster) error {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-controller", cluster.Name),
			Namespace: cluster.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
		if err := controllerutil.SetControllerReference(cluster, sts, r.Scheme); err != nil {
			return err
		}

		sts.Labels = r.labelsForCluster(cluster, "controller")

		replicas := int32(3)
		if cluster.Spec.Controller.Replicas != nil {
			replicas = *cluster.Spec.Controller.Replicas
		}

		sts.Spec = appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: fmt.Sprintf("%s-controller-headless", cluster.Name),
			Selector: &metav1.LabelSelector{
				MatchLabels: r.labelsForCluster(cluster, "controller"),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: r.labelsForCluster(cluster, "controller"),
				},
				Spec: corev1.PodSpec{
					Affinity:     cluster.Spec.Controller.Affinity,
					Tolerations:  cluster.Spec.Controller.Tolerations,
					NodeSelector: cluster.Spec.Controller.NodeSelector,
					Containers: []corev1.Container{
						{
							Name:  "controller",
							Image: "aethermq/controller:latest",
							Ports: []corev1.ContainerPort{
								{ContainerPort: 19091, Name: "api"},
								{ContainerPort: 19092, Name: "raft"},
							},
							Env: []corev1.EnvVar{
								{Name: "HELIOS_NODE_ID", ValueFrom: &corev1.EnvVarSource{
									FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
								}},
								{Name: "HELIOS_LISTEN", Value: "0.0.0.0:19091"},
								{Name: "HELIOS_RAFT_ADDR", Value: "0.0.0.0:19092"},
								{Name: "HELIOS_DATA_DIR", Value: "/data"},
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "data", MountPath: "/data"},
							},
							Resources: r.resourceRequirements(cluster.Spec.Controller.Resources),
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/health",
										Port: intstr.FromInt(19091),
									},
								},
								InitialDelaySeconds: 30,
								PeriodSeconds:       10,
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/ready",
										Port: intstr.FromInt(19091),
									},
								},
								InitialDelaySeconds: 10,
								PeriodSeconds:       5,
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "data",
								},
							},
						},
					},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "data",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse(cluster.Spec.Storage.Size),
							},
						},
						StorageClassName: &cluster.Spec.Storage.StorageClass,
					},
				},
			},
		}

		return nil
	})

	return err
}

func (r *AetherMQClusterReconciler) reconcileBroker(ctx context.Context, cluster *aethermqv1.AetherMQCluster) error {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-broker", cluster.Name),
			Namespace: cluster.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
		if err := controllerutil.SetControllerReference(cluster, sts, r.Scheme); err != nil {
			return err
		}

		sts.Labels = r.labelsForCluster(cluster, "broker")

		replicas := int32(3)
		if cluster.Spec.Broker.Replicas != nil {
			replicas = *cluster.Spec.Broker.Replicas
		}

		sts.Spec = appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: fmt.Sprintf("%s-broker-headless", cluster.Name),
			Selector: &metav1.LabelSelector{
				MatchLabels: r.labelsForCluster(cluster, "broker"),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: r.labelsForCluster(cluster, "broker"),
				},
				Spec: corev1.PodSpec{
					Affinity:     cluster.Spec.Broker.Affinity,
					Tolerations:  cluster.Spec.Broker.Tolerations,
					NodeSelector: cluster.Spec.Broker.NodeSelector,
					Containers: []corev1.Container{
						{
							Name:  "broker",
							Image: "aethermq/broker:latest",
							Ports: []corev1.ContainerPort{
								{ContainerPort: 9092, Name: "api"},
								{ContainerPort: 9093, Name: "metrics"},
							},
							Env: []corev1.EnvVar{
								{Name: "HELIOS_NODE_ID", ValueFrom: &corev1.EnvVarSource{
									FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
								}},
								{Name: "HELIOS_LISTEN", Value: "0.0.0.0:9092"},
								{Name: "HELIOS_DATA_DIR", Value: "/data"},
								{Name: "HELIOS_CONTROLLER", Value: fmt.Sprintf("%s-controller:19091", cluster.Name)},
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "data", MountPath: "/data"},
							},
							Resources: r.resourceRequirements(cluster.Spec.Broker.Resources),
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/health",
										Port: intstr.FromInt(9093),
									},
								},
								InitialDelaySeconds: 30,
								PeriodSeconds:       10,
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/ready",
										Port: intstr.FromInt(9093),
									},
								},
								InitialDelaySeconds: 10,
								PeriodSeconds:       5,
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "data",
								},
							},
						},
					},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "data",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse(cluster.Spec.Storage.Size),
							},
						},
						StorageClassName: &cluster.Spec.Storage.StorageClass,
					},
				},
			},
		}

		return nil
	})

	return err
}

func (r *AetherMQClusterReconciler) reconcileGateway(ctx context.Context, cluster *aethermqv1.AetherMQCluster) error {
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-gateway", cluster.Name),
			Namespace: cluster.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, deploy, func() error {
		if err := controllerutil.SetControllerReference(cluster, deploy, r.Scheme); err != nil {
			return err
		}

		deploy.Labels = r.labelsForCluster(cluster, "gateway")

		replicas := int32(2)
		if cluster.Spec.Gateway.Replicas != nil {
			replicas = *cluster.Spec.Gateway.Replicas
		}

		deploy.Spec = appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: r.labelsForCluster(cluster, "gateway"),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: r.labelsForCluster(cluster, "gateway"),
				},
				Spec: corev1.PodSpec{
					Affinity:     cluster.Spec.Gateway.Affinity,
					Tolerations:  cluster.Spec.Gateway.Tolerations,
					NodeSelector: cluster.Spec.Gateway.NodeSelector,
					Containers: []corev1.Container{
						{
							Name:  "gateway",
							Image: "aethermq/gateway:latest",
							Ports: []corev1.ContainerPort{
								{ContainerPort: 29092, Name: "api"},
								{ContainerPort: 9093, Name: "metrics"},
							},
							Env: []corev1.EnvVar{
								{Name: "HELIOS_NODE_ID", ValueFrom: &corev1.EnvVarSource{
									FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
								}},
								{Name: "HELIOS_LISTEN", Value: "0.0.0.0:29092"},
								{Name: "HELIOS_CONTROLLER", Value: fmt.Sprintf("%s-controller:19091", cluster.Name)},
							},
							Resources: r.resourceRequirements(cluster.Spec.Gateway.Resources),
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/health",
										Port: intstr.FromInt(9093),
									},
								},
								InitialDelaySeconds: 30,
								PeriodSeconds:       10,
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/ready",
										Port: intstr.FromInt(9093),
									},
								},
								InitialDelaySeconds: 10,
								PeriodSeconds:       5,
							},
						},
					},
				},
			},
		}

		return nil
	})

	return err
}

func (r *AetherMQClusterReconciler) reconcileServices(ctx context.Context, cluster *aethermqv1.AetherMQCluster) error {
	services := []*corev1.Service{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-controller-headless", cluster.Name),
				Namespace: cluster.Namespace,
			},
			Spec: corev1.ServiceSpec{
				ClusterIP: "None",
				Selector:  r.labelsForCluster(cluster, "controller"),
				Ports: []corev1.ServicePort{
					{Name: "api", Port: 19091},
					{Name: "raft", Port: 19092},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-broker-headless", cluster.Name),
				Namespace: cluster.Namespace,
			},
			Spec: corev1.ServiceSpec{
				ClusterIP: "None",
				Selector:  r.labelsForCluster(cluster, "broker"),
				Ports: []corev1.ServicePort{
					{Name: "api", Port: 9092},
					{Name: "metrics", Port: 9093},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-gateway", cluster.Name),
				Namespace: cluster.Namespace,
			},
			Spec: corev1.ServiceSpec{
				Type:     cluster.Spec.Gateway.Service.Type,
				Selector: r.labelsForCluster(cluster, "gateway"),
				Ports: []corev1.ServicePort{
					{Name: "api", Port: 29092, NodePort: cluster.Spec.Gateway.Service.NodePort},
					{Name: "metrics", Port: 9093},
				},
			},
		},
	}

	for _, svc := range services {
		if err := controllerutil.SetControllerReference(cluster, svc, r.Scheme); err != nil {
			return err
		}
		if err := r.Create(ctx, svc); err != nil && !errors.IsAlreadyExists(err) {
			return err
		}
	}

	return nil
}

func (r *AetherMQClusterReconciler) reconcileConfigMaps(ctx context.Context, cluster *aethermqv1.AetherMQCluster) error {
	return nil
}

func (r *AetherMQClusterReconciler) updateStatus(ctx context.Context, cluster *aethermqv1.AetherMQCluster) error {
	controllerSts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      fmt.Sprintf("%s-controller", cluster.Name),
		Namespace: cluster.Namespace,
	}, controllerSts); err == nil {
		cluster.Status.ControllerStatus = &aethermqv1.ComponentStatus{
			Replicas:      controllerSts.Status.Replicas,
			ReadyReplicas: controllerSts.Status.ReadyReplicas,
		}
	}

	brokerSts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      fmt.Sprintf("%s-broker", cluster.Name),
		Namespace: cluster.Namespace,
	}, brokerSts); err == nil {
		cluster.Status.BrokerStatus = &aethermqv1.ComponentStatus{
			Replicas:      brokerSts.Status.Replicas,
			ReadyReplicas: brokerSts.Status.ReadyReplicas,
		}
	}

	gatewayDeploy := &appsv1.Deployment{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      fmt.Sprintf("%s-gateway", cluster.Name),
		Namespace: cluster.Namespace,
	}, gatewayDeploy); err == nil {
		cluster.Status.GatewayStatus = &aethermqv1.ComponentStatus{
			Replicas:      gatewayDeploy.Status.Replicas,
			ReadyReplicas: gatewayDeploy.Status.ReadyReplicas,
		}
	}

	if cluster.Status.ControllerStatus != nil &&
		cluster.Status.BrokerStatus != nil &&
		cluster.Status.GatewayStatus != nil &&
		cluster.Status.ControllerStatus.ReadyReplicas == cluster.Status.ControllerStatus.Replicas &&
		cluster.Status.BrokerStatus.ReadyReplicas == cluster.Status.BrokerStatus.Replicas &&
		cluster.Status.GatewayStatus.ReadyReplicas == cluster.Status.GatewayStatus.Replicas {
		cluster.Status.Phase = "Running"
	} else {
		cluster.Status.Phase = "Creating"
	}

	return r.Status().Update(ctx, cluster)
}

func (r *AetherMQClusterReconciler) cleanupCluster(ctx context.Context, cluster *aethermqv1.AetherMQCluster) error {
	return nil
}

func (r *AetherMQClusterReconciler) labelsForCluster(cluster *aethermqv1.AetherMQCluster, component string) map[string]string {
	return map[string]string{
		appLabel:       "aethermq",
		componentLabel: component,
		instanceLabel:  cluster.Name,
		versionLabel:   "latest",
	}
}

func (r *AetherMQClusterReconciler) resourceRequirements(reqs aethermqv1.ResourceRequirements) corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList(reqs.Requests),
		Limits:   corev1.ResourceList(reqs.Limits),
	}
}

func (r *AetherMQClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aethermqv1.AetherMQCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Complete(r)
}
