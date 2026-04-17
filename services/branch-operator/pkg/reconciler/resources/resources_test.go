package resources_test

import (
	"testing"

	"xata/services/branch-operator/pkg/reconciler/resources"

	barmanApi "github.com/cloudnative-pg/barman-cloud/pkg/api"
	barmanPluginApi "github.com/cloudnative-pg/plugin-barman-cloud/api/v1"
	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	v1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestNetworkPolicySpec(t *testing.T) {
	t.Parallel()

	testcases := []string{
		"test-branch-1",
		"test-branch-2",
	}

	dnsPort := intstr.FromInt(53)
	dnsProtocolUDP := v1.ProtocolUDP
	dnsProtocolTCP := v1.ProtocolTCP

	for _, branchName := range testcases {
		t.Run(branchName, func(t *testing.T) {
			spec := resources.NetworkPolicySpec(branchName)

			matchLabels := map[string]string{
				"cnpg.io/cluster": branchName,
			}

			expected := networkingv1.NetworkPolicySpec{
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

			require.Equal(t, expected, spec)
		})
	}
}

func TestClustersServiceSpec(t *testing.T) {
	t.Parallel()

	spec := resources.ClustersServiceSpec()

	expected := v1.ServiceSpec{
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

	require.Equal(t, expected, spec)
}

func TestAdditionalServiceSpec(t *testing.T) {
	t.Parallel()

	testcases := map[string]struct {
		clusterName  string
		selectorType string
		want         v1.ServiceSpec
	}{
		"rw targets primary instance": {
			clusterName:  "test-cluster",
			selectorType: "rw",
			want: v1.ServiceSpec{
				Type: v1.ServiceTypeClusterIP,
				Ports: []v1.ServicePort{
					{
						Name:       "postgres",
						Port:       5432,
						TargetPort: intstr.FromInt(5432),
						Protocol:   v1.ProtocolTCP,
					},
				},
				Selector: map[string]string{
					"cnpg.io/cluster":      "test-cluster",
					"cnpg.io/instanceRole": "primary",
				},
			},
		},
		"r targets all instances": {
			clusterName:  "test-cluster",
			selectorType: "r",
			want: v1.ServiceSpec{
				Type: v1.ServiceTypeClusterIP,
				Ports: []v1.ServicePort{
					{
						Name:       "postgres",
						Port:       5432,
						TargetPort: intstr.FromInt(5432),
						Protocol:   v1.ProtocolTCP,
					},
				},
				Selector: map[string]string{
					"cnpg.io/cluster": "test-cluster",
					"cnpg.io/podRole": "instance",
				},
			},
		},
		"ro targets replica instances": {
			clusterName:  "test-cluster",
			selectorType: "ro",
			want: v1.ServiceSpec{
				Type: v1.ServiceTypeClusterIP,
				Ports: []v1.ServicePort{
					{
						Name:       "postgres",
						Port:       5432,
						TargetPort: intstr.FromInt(5432),
						Protocol:   v1.ProtocolTCP,
					},
				},
				Selector: map[string]string{
					"cnpg.io/cluster":      "test-cluster",
					"cnpg.io/instanceRole": "replica",
				},
			},
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			got := resources.AdditionalServiceSpec(tc.clusterName, tc.selectorType)

			require.Equal(t, tc.want, got)
		})
	}
}

func TestPoolerServiceSpec(t *testing.T) {
	t.Parallel()

	testcases := map[string]struct {
		poolerName string
		want       v1.ServiceSpec
	}{
		"routes to pooler pods": {
			poolerName: "test-branch-1-pooler",
			want: v1.ServiceSpec{
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
					"cnpg.io/poolerName": "test-branch-1-pooler",
				},
			},
		},
		"different pooler name": {
			poolerName: "test-branch-2-pooler",
			want: v1.ServiceSpec{
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
					"cnpg.io/poolerName": "test-branch-2-pooler",
				},
			},
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			got := resources.PoolerServiceSpec(tc.poolerName)

			require.Equal(t, tc.want, got)
		})
	}
}

func TestObjectStoreSpec(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		name            string
		backupsBucket   string
		backupsEndpoint string
		retention       string
		want            barmanPluginApi.ObjectStoreSpec
	}{
		{
			name:            "production mode with IAM role - bucket A",
			backupsBucket:   "s3://prod-backup-bucket/path/to/backups",
			backupsEndpoint: "",
			retention:       "60d",
			want: barmanPluginApi.ObjectStoreSpec{
				RetentionPolicy: "60d",
				Configuration: apiv1.BarmanObjectStoreConfiguration{
					DestinationPath: "s3://prod-backup-bucket/path/to/backups",
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
			},
		},
		{
			name:            "production mode with IAM role - bucket B",
			backupsBucket:   "s3://another-prod-bucket/different/path",
			backupsEndpoint: "",
			retention:       "30d",
			want: barmanPluginApi.ObjectStoreSpec{
				RetentionPolicy: "30d",
				Configuration: apiv1.BarmanObjectStoreConfiguration{
					DestinationPath: "s3://another-prod-bucket/different/path",
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
			},
		},
		{
			name:            "local mode with MinIO - bucket C",
			backupsBucket:   "s3://dev-bucket/backups",
			backupsEndpoint: "http://minio.local:9000",
			retention:       "7d",
			want: barmanPluginApi.ObjectStoreSpec{
				RetentionPolicy: "7d",
				Configuration: apiv1.BarmanObjectStoreConfiguration{
					DestinationPath: "s3://dev-bucket/backups",
					EndpointURL:     "http://minio.local:9000",
					BarmanCredentials: apiv1.BarmanCredentials{
						AWS: &apiv1.S3Credentials{
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
			},
		},
		{
			name:            "local mode with MinIO - bucket D",
			backupsBucket:   "s3://test-bucket/test/path",
			backupsEndpoint: "http://minio-test.cluster.local:9000",
			retention:       "14d",
			want: barmanPluginApi.ObjectStoreSpec{
				RetentionPolicy: "14d",
				Configuration: apiv1.BarmanObjectStoreConfiguration{
					DestinationPath: "s3://test-bucket/test/path",
					EndpointURL:     "http://minio-test.cluster.local:9000",
					BarmanCredentials: apiv1.BarmanCredentials{
						AWS: &apiv1.S3Credentials{
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
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			got := resources.ObjectStoreSpec(tc.backupsBucket, tc.backupsEndpoint, tc.retention)

			require.Equal(t, tc.want, got)
		})
	}
}

func TestScheduledBackupSpec(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		name       string
		branchName string
		schedule   string
		suspend    bool
		want       apiv1.ScheduledBackupSpec
	}{
		{
			name:       "scheduled backup with hourly schedule",
			branchName: "test-branch-1",
			schedule:   "0 0 * * * *",
			want: apiv1.ScheduledBackupSpec{
				Cluster: apiv1.LocalObjectReference{
					Name: "test-branch-1",
				},
				Method:    apiv1.BackupMethodPlugin,
				Schedule:  "0 0 * * * *",
				Immediate: new(true),
				Suspend:   new(false),
				PluginConfiguration: &apiv1.BackupPluginConfiguration{
					Name: "barman-cloud.cloudnative-pg.io",
				},
			},
		},
		{
			name:       "scheduled backup with very frequent schedule",
			branchName: "test-branch-2",
			schedule:   "0 * * * * *",
			want: apiv1.ScheduledBackupSpec{
				Cluster: apiv1.LocalObjectReference{
					Name: "test-branch-2",
				},
				Method:    apiv1.BackupMethodPlugin,
				Schedule:  "0 * * * * *",
				Immediate: new(true),
				Suspend:   new(false),
				PluginConfiguration: &apiv1.BackupPluginConfiguration{
					Name: "barman-cloud.cloudnative-pg.io",
				},
			},
		},
		{
			name:       "suspended scheduled backup",
			branchName: "test-branch-2",
			schedule:   "0 * * * * *",
			suspend:    true,
			want: apiv1.ScheduledBackupSpec{
				Cluster: apiv1.LocalObjectReference{
					Name: "test-branch-2",
				},
				Method:    apiv1.BackupMethodPlugin,
				Schedule:  "0 * * * * *",
				Immediate: new(true),
				Suspend:   new(true),
				PluginConfiguration: &apiv1.BackupPluginConfiguration{
					Name: "barman-cloud.cloudnative-pg.io",
				},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			got := resources.ScheduledBackupSpec(tc.branchName, tc.schedule, tc.suspend)

			require.Equal(t, tc.want, got)
		})
	}
}

func TestPoolerSpec(t *testing.T) {
	t.Parallel()

	testcases := map[string]struct {
		clusterName      string
		instances        int32
		hibernated       bool
		poolMode         apiv1.PgBouncerPoolMode
		maxClientConn    string
		defaultPoolSize  string
		podLabels        map[string]string
		imagePullSecrets []string
		want             apiv1.PoolerSpec
	}{
		"active branch": {
			clusterName:   "test-branch-1",
			instances:     1,
			hibernated:    false,
			poolMode:      apiv1.PgBouncerPoolModeSession,
			maxClientConn: "100",
			want: apiv1.PoolerSpec{
				Cluster: apiv1.LocalObjectReference{
					Name: "test-branch-1",
				},
				Type:      apiv1.PoolerTypeRW,
				Instances: new(int32(1)),
				PgBouncer: &apiv1.PgBouncerSpec{
					PoolMode: apiv1.PgBouncerPoolModeSession,
					Parameters: map[string]string{
						"max_client_conn":         "100",
						"max_prepared_statements": "1000",
						"query_wait_timeout":      "120",
						"server_idle_timeout":     "60",
					},
				},
				ServiceTemplate: &apiv1.ServiceTemplateSpec{
					ObjectMeta: apiv1.Metadata{
						Annotations: resources.InheritedAnnotations,
					},
				},
				Template: &apiv1.PodTemplateSpec{
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name: "pgbouncer",
								Ports: []v1.ContainerPort{
									{
										Name:          resources.PoolerMetricsPortName,
										ContainerPort: resources.PoolerMetricsPort,
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
					},
				},
			},
		},
		"hibernated branch": {
			clusterName:   "test-branch-2",
			instances:     1,
			hibernated:    true,
			poolMode:      apiv1.PgBouncerPoolModeSession,
			maxClientConn: "100",
			want: apiv1.PoolerSpec{
				Cluster: apiv1.LocalObjectReference{
					Name: "test-branch-2",
				},
				Type:      apiv1.PoolerTypeRW,
				Instances: new(int32(0)),
				PgBouncer: &apiv1.PgBouncerSpec{
					PoolMode: apiv1.PgBouncerPoolModeSession,
					Parameters: map[string]string{
						"max_client_conn":         "100",
						"max_prepared_statements": "1000",
						"query_wait_timeout":      "120",
						"server_idle_timeout":     "60",
					},
				},
				ServiceTemplate: &apiv1.ServiceTemplateSpec{
					ObjectMeta: apiv1.Metadata{
						Annotations: resources.InheritedAnnotations,
					},
				},
				Template: &apiv1.PodTemplateSpec{
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name: "pgbouncer",
								Ports: []v1.ContainerPort{
									{
										Name:          resources.PoolerMetricsPortName,
										ContainerPort: resources.PoolerMetricsPort,
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
					},
				},
			},
		},
		"with pod labels": {
			clusterName:   "test-branch-3",
			instances:     1,
			hibernated:    false,
			poolMode:      apiv1.PgBouncerPoolModeSession,
			maxClientConn: "100",
			podLabels: map[string]string{
				"xata.io/organizationID": "org-123",
				"xata.io/projectID":      "proj-456",
			},
			want: apiv1.PoolerSpec{
				Cluster: apiv1.LocalObjectReference{
					Name: "test-branch-3",
				},
				Type:      apiv1.PoolerTypeRW,
				Instances: new(int32(1)),
				PgBouncer: &apiv1.PgBouncerSpec{
					PoolMode: apiv1.PgBouncerPoolModeSession,
					Parameters: map[string]string{
						"max_client_conn":         "100",
						"max_prepared_statements": "1000",
						"query_wait_timeout":      "120",
						"server_idle_timeout":     "60",
					},
				},
				ServiceTemplate: &apiv1.ServiceTemplateSpec{
					ObjectMeta: apiv1.Metadata{
						Annotations: resources.InheritedAnnotations,
					},
				},
				Template: &apiv1.PodTemplateSpec{
					ObjectMeta: apiv1.Metadata{
						Labels: map[string]string{
							"xata.io/organizationID": "org-123",
							"xata.io/projectID":      "proj-456",
						},
					},
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name: "pgbouncer",
								Ports: []v1.ContainerPort{
									{
										Name:          resources.PoolerMetricsPortName,
										ContainerPort: resources.PoolerMetricsPort,
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
					},
				},
			},
		},
		"with image pull secrets": {
			clusterName:      "test-branch-4",
			instances:        1,
			hibernated:       false,
			poolMode:         apiv1.PgBouncerPoolModeSession,
			maxClientConn:    "100",
			imagePullSecrets: []string{"ghcr-secret", "ecr-secret"},
			want: apiv1.PoolerSpec{
				Cluster: apiv1.LocalObjectReference{
					Name: "test-branch-4",
				},
				Type:      apiv1.PoolerTypeRW,
				Instances: new(int32(1)),
				PgBouncer: &apiv1.PgBouncerSpec{
					PoolMode: apiv1.PgBouncerPoolModeSession,
					Parameters: map[string]string{
						"max_client_conn":         "100",
						"max_prepared_statements": "1000",
						"query_wait_timeout":      "120",
						"server_idle_timeout":     "60",
					},
				},
				ServiceTemplate: &apiv1.ServiceTemplateSpec{
					ObjectMeta: apiv1.Metadata{
						Annotations: resources.InheritedAnnotations,
					},
				},
				Template: &apiv1.PodTemplateSpec{
					Spec: v1.PodSpec{
						ImagePullSecrets: []v1.LocalObjectReference{
							{Name: "ghcr-secret"},
							{Name: "ecr-secret"},
						},
						Containers: []v1.Container{
							{
								Name: "pgbouncer",
								Ports: []v1.ContainerPort{
									{
										Name:          resources.PoolerMetricsPortName,
										ContainerPort: resources.PoolerMetricsPort,
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
					},
				},
			},
		},
		"with default_pool_size override": {
			clusterName:     "test-branch-5",
			instances:       1,
			hibernated:      false,
			poolMode:        apiv1.PgBouncerPoolModeTransaction,
			maxClientConn:   "10000",
			defaultPoolSize: "180",
			want: apiv1.PoolerSpec{
				Cluster: apiv1.LocalObjectReference{
					Name: "test-branch-5",
				},
				Type:      apiv1.PoolerTypeRW,
				Instances: new(int32(1)),
				PgBouncer: &apiv1.PgBouncerSpec{
					PoolMode: apiv1.PgBouncerPoolModeTransaction,
					Parameters: map[string]string{
						"max_client_conn":         "10000",
						"max_prepared_statements": "1000",
						"query_wait_timeout":      "120",
						"default_pool_size":       "180",
						"server_idle_timeout":     "60",
					},
				},
				ServiceTemplate: &apiv1.ServiceTemplateSpec{
					ObjectMeta: apiv1.Metadata{
						Annotations: resources.InheritedAnnotations,
					},
				},
				Template: &apiv1.PodTemplateSpec{
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name: "pgbouncer",
								Ports: []v1.ContainerPort{
									{
										Name:          resources.PoolerMetricsPortName,
										ContainerPort: resources.PoolerMetricsPort,
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
					},
				},
			},
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			got := resources.PoolerSpec(tc.clusterName, tc.instances, tc.hibernated, tc.poolMode, tc.maxClientConn, tc.defaultPoolSize, tc.podLabels, tc.imagePullSecrets)

			require.Equal(t, tc.want, got)
		})
	}
}

func TestVolumeSnapshotSpec(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		name          string
		sourcePVC     string
		snapshotClass string
		want          snapshotv1.VolumeSnapshotSpec
	}{
		{
			name:          "source PVC and snapshot class",
			sourcePVC:     "some-pvc-name",
			snapshotClass: "some-snapshot-class",
			want: snapshotv1.VolumeSnapshotSpec{
				VolumeSnapshotClassName: new("some-snapshot-class"),
				Source: snapshotv1.VolumeSnapshotSource{
					PersistentVolumeClaimName: new("some-pvc-name"),
				},
			},
		},
		{
			name:          "different PVC and snapshot class",
			sourcePVC:     "some-other-pvc-name",
			snapshotClass: "some-other-snapshot-class",
			want: snapshotv1.VolumeSnapshotSpec{
				VolumeSnapshotClassName: new("some-other-snapshot-class"),
				Source: snapshotv1.VolumeSnapshotSource{
					PersistentVolumeClaimName: new("some-other-pvc-name"),
				},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			got := resources.VolumeSnapshotSpec(tc.sourcePVC, tc.snapshotClass)

			require.Equal(t, tc.want, got)
		})
	}
}

func TestSecret(t *testing.T) {
	t.Parallel()

	testcases := map[string]struct {
		name      string
		namespace string
		username  string
		password  string
		want      *v1.Secret
	}{
		"superuser secret": {
			name:      "branch-1-superuser",
			namespace: "xata-clusters",
			username:  "postgres",
			password:  "supersecret",
			want: &v1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "branch-1-superuser",
					Namespace: "xata-clusters",
				},
				Type: v1.SecretTypeBasicAuth,
				Data: map[string][]byte{
					v1.BasicAuthUsernameKey: []byte("postgres"),
					v1.BasicAuthPasswordKey: []byte("supersecret"),
				},
			},
		},
		"app secret": {
			name:      "branch-1-app",
			namespace: "xata-clusters",
			username:  "xata",
			password:  "appsecret",
			want: &v1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "branch-1-app",
					Namespace: "xata-clusters",
				},
				Type: v1.SecretTypeBasicAuth,
				Data: map[string][]byte{
					v1.BasicAuthUsernameKey: []byte("xata"),
					v1.BasicAuthPasswordKey: []byte("appsecret"),
				},
			},
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			got := resources.Secret(tc.name, tc.namespace, tc.username, tc.password)

			require.Equal(t, tc.want, got)
		})
	}
}
