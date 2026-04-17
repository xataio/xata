package resources

import (
	barmanApi "github.com/cloudnative-pg/barman-cloud/pkg/api"
	barmanPluginApi "github.com/cloudnative-pg/plugin-barman-cloud/api/v1"
	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	v1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	PoolerMetricsPort     int32  = 9127
	PoolerMetricsPortName string = "metrics-pooler"
)

// NetworkPolicySpec defines the NetworkPolicySpec for the given cluster name.
// It allows ingress and egress traffic only between pods within the same
// cluster.
func NetworkPolicySpec(clusterName string) networkingv1.NetworkPolicySpec {
	matchLabels := map[string]string{
		"cnpg.io/cluster": clusterName,
	}

	dnsPort := intstr.FromInt(53)
	dnsProtocolUDP := v1.ProtocolUDP
	dnsProtocolTCP := v1.ProtocolTCP

	return networkingv1.NetworkPolicySpec{
		PodSelector: metav1.LabelSelector{MatchLabels: matchLabels},
		Ingress: []networkingv1.NetworkPolicyIngressRule{
			{
				From: []networkingv1.NetworkPolicyPeer{
					{PodSelector: &metav1.LabelSelector{MatchLabels: matchLabels}},
				},
			},
		},
		Egress: []networkingv1.NetworkPolicyEgressRule{
			{
				To: []networkingv1.NetworkPolicyPeer{
					{PodSelector: &metav1.LabelSelector{MatchLabels: matchLabels}},
				},
			},
			{
				Ports: []networkingv1.NetworkPolicyPort{
					{Port: &dnsPort, Protocol: &dnsProtocolUDP},
					{Port: &dnsPort, Protocol: &dnsProtocolTCP},
				},
			},
		},
		PolicyTypes: []networkingv1.PolicyType{
			networkingv1.PolicyTypeIngress,
			networkingv1.PolicyTypeEgress,
		},
	}
}

// ClustersServiceSpec defines the ServiceSpec for a branch-specific clusters
// Service.
//
// Each `Branch` needs a branch-specific `clusters` Service to be created
// in the `xata` namespace. The service routes traffic to the `clusters`
// service.
//
// This service is duplicated into the primary cell on branch creation,
// orchestrated by the control plane, so that cross-cell access to the
// correct `clusters` service for each branch is possible.
func ClustersServiceSpec() v1.ServiceSpec {
	return v1.ServiceSpec{
		Type: v1.ServiceTypeClusterIP,
		Ports: []v1.ServicePort{
			{
				Name:       "grpc",
				Port:       5002,
				TargetPort: intstr.FromInt(5002),
				Protocol:   v1.ProtocolTCP,
			},
		},
		Selector: map[string]string{
			"app": "clusters",
		},
	}
}

// AdditionalServiceSpec defines the ServiceSpec for a branch-specific
// additional service that routes directly to CNPG PostgreSQL pods.
//
// The selectorType determines which pods the service targets:
//   - "rw": primary instance (read-write)
//   - "r":  all instances (read)
//   - "ro": replica instances (read-only)
func AdditionalServiceSpec(clusterName, selectorType string) v1.ServiceSpec {
	selector := map[string]string{
		"cnpg.io/cluster": clusterName,
	}

	switch selectorType {
	case "rw":
		selector["cnpg.io/instanceRole"] = "primary"
	case "r":
		selector["cnpg.io/podRole"] = "instance"
	case "ro":
		selector["cnpg.io/instanceRole"] = "replica"
	}

	return v1.ServiceSpec{
		Type: v1.ServiceTypeClusterIP,
		Ports: []v1.ServicePort{
			{
				Name:       "postgres",
				Port:       5432,
				TargetPort: intstr.FromInt(5432),
				Protocol:   v1.ProtocolTCP,
			},
		},
		Selector: selector,
	}
}

// PoolerServiceSpec defines the ServiceSpec for a branch-specific additional
// service that routes to PgBouncer pooler pods.
func PoolerServiceSpec(poolerName string) v1.ServiceSpec {
	return v1.ServiceSpec{
		Type: v1.ServiceTypeClusterIP,
		Ports: []v1.ServicePort{
			{
				Name:       "pgbouncer",
				Port:       5432,
				TargetPort: intstr.FromInt(5432),
				Protocol:   v1.ProtocolTCP,
			},
		},
		Selector: map[string]string{
			"cnpg.io/podRole":    "pooler",
			"cnpg.io/poolerName": poolerName,
		},
	}
}

// ObjectStoreSpec defines the ObjectStoreSpec for a branch's backup storage.
// It configures S3/MinIO storage for CNPG backup retention.
//
// It supports both production (AWS S3 with IAM role) and local dev (MinIO with
// credentials) modes based on whether backupsEndpoint is set.
func ObjectStoreSpec(backupsBucket, backupsEndpoint, retention string) barmanPluginApi.ObjectStoreSpec {
	spec := barmanPluginApi.ObjectStoreSpec{
		RetentionPolicy: retention,
		Configuration: apiv1.BarmanObjectStoreConfiguration{
			DestinationPath: backupsBucket,
			BarmanCredentials: apiv1.BarmanCredentials{
				AWS: &apiv1.S3Credentials{
					InheritFromIAMRole: true,
				},
			},
			Wal: &apiv1.WalBackupConfiguration{
				Compression: barmanApi.CompressionTypeGzip,
			},
			Data: &apiv1.DataBackupConfiguration{
				Compression:           barmanApi.CompressionTypeGzip,
				AdditionalCommandArgs: []string{"--min-chunk-size=5MB", "--read-timeout=60", "-vv"},
			},
		},
		InstanceSidecarConfiguration: barmanPluginApi.InstanceSidecarConfiguration{
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("100m"),
					v1.ResourceMemory: resource.MustParse("512Mi"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("250m"),
					v1.ResourceMemory: resource.MustParse("512Mi"),
				},
			},
			RetentionPolicyIntervalSeconds: 86400,
		},
	}

	// Configure MinIO for local development if endpoint is provided
	if backupsEndpoint != "" {
		spec.Configuration.EndpointURL = backupsEndpoint
		spec.Configuration.AWS = &apiv1.S3Credentials{
			AccessKeyIDReference: &apiv1.SecretKeySelector{
				LocalObjectReference: apiv1.LocalObjectReference{
					Name: "minio-eu",
				},
				Key: "rootUser",
			},
			SecretAccessKeyReference: &apiv1.SecretKeySelector{
				LocalObjectReference: apiv1.LocalObjectReference{
					Name: "minio-eu",
				},
				Key: "rootPassword",
			},
			InheritFromIAMRole: false,
		}
	}

	return spec
}

// ScheduledBackupSpec defines the ScheduledBackupSpec for a branch's scheduled backups.
// It configures automated backups using the barman-cloud plugin.
func ScheduledBackupSpec(clusterName, schedule string, suspend bool) apiv1.ScheduledBackupSpec {
	return apiv1.ScheduledBackupSpec{
		Cluster: apiv1.LocalObjectReference{
			Name: clusterName,
		},
		Method:    apiv1.BackupMethodPlugin,
		Schedule:  schedule,
		Immediate: new(true),
		Suspend:   new(suspend),
		PluginConfiguration: &apiv1.BackupPluginConfiguration{
			Name: "barman-cloud.cloudnative-pg.io",
		},
	}
}

// Secret builds a BasicAuth Secret with the given name, username, and password.
// Owner references and labels are set by the reconciler.
func Secret(name, namespace, username, password string) *v1.Secret {
	return &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Type: v1.SecretTypeBasicAuth,
		Data: map[string][]byte{
			v1.BasicAuthUsernameKey: []byte(username),
			v1.BasicAuthPasswordKey: []byte(password),
		},
	}
}

// PoolerSpec defines the PoolerSpec for a CNPG PgBouncer connection pooler.
// It creates a PgBouncer instance in transaction mode with max_client_conn
// set to the maximum. When hibernated, instances is set to 0.
// When defaultPoolSize is non-empty, it is set verbatim on PgBouncer.
func PoolerSpec(clusterName string, instances int32, hibernated bool, poolMode apiv1.PgBouncerPoolMode, maxClientConn, defaultPoolSize string, podLabels map[string]string, imagePullSecrets []string) apiv1.PoolerSpec {
	if hibernated {
		instances = 0
	}

	params := map[string]string{
		"max_client_conn":         maxClientConn,
		"max_prepared_statements": "1000",
		"query_wait_timeout":      "120",
		"server_idle_timeout":     "60",
	}
	if defaultPoolSize != "" {
		params["default_pool_size"] = defaultPoolSize
	}

	spec := apiv1.PoolerSpec{
		Cluster: apiv1.LocalObjectReference{
			Name: clusterName,
		},
		Type:      apiv1.PoolerTypeRW,
		Instances: new(instances),
		PgBouncer: &apiv1.PgBouncerSpec{
			PoolMode:   poolMode,
			Parameters: params,
		},
		ServiceTemplate: &apiv1.ServiceTemplateSpec{
			ObjectMeta: apiv1.Metadata{
				Annotations: InheritedAnnotations,
			},
		},
	}

	var pullSecrets []v1.LocalObjectReference
	for _, name := range imagePullSecrets {
		pullSecrets = append(pullSecrets, v1.LocalObjectReference{Name: name})
	}

	podSpec := v1.PodSpec{
		ImagePullSecrets: pullSecrets,
		Containers: []v1.Container{
			{
				Name: "pgbouncer",
				Ports: []v1.ContainerPort{
					{
						Name:          PoolerMetricsPortName,
						ContainerPort: PoolerMetricsPort,
						Protocol:      v1.ProtocolTCP,
					},
				},
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("200m"),
						v1.ResourceMemory: resource.MustParse("100Mi"),
					},
					Limits: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("500m"),
						v1.ResourceMemory: resource.MustParse("100Mi"),
					},
				},
			},
		},
	}

	spec.Template = &apiv1.PodTemplateSpec{
		ObjectMeta: apiv1.Metadata{
			Labels: podLabels,
		},
		Spec: podSpec,
	}

	return spec
}

// VolumeSnapshotSpec defines the VolumeSnapshotSpec for a branch created from a
// parent branch. It configures the snapshot to be taken from the parent's PVC.
func VolumeSnapshotSpec(sourcePVC, volumeSnapshotClass string) snapshotv1.VolumeSnapshotSpec {
	return snapshotv1.VolumeSnapshotSpec{
		VolumeSnapshotClassName: new(volumeSnapshotClass),
		Source: snapshotv1.VolumeSnapshotSource{
			PersistentVolumeClaimName: new(sourcePVC),
		},
	}
}
