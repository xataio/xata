package resources

import (
	"xata/services/branch-operator/api/v1alpha1"

	barmanApi "github.com/cloudnative-pg/barman-cloud/pkg/api"
	barmanPluginApi "github.com/cloudnative-pg/plugin-barman-cloud/api/v1"
	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	apiv1ac "github.com/xataio/xata-cnpg/pkg/client/applyconfiguration/api/v1"
	v1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
)

// ScaleToZeroClusterRoleRules defines the cluster-scoped rules the
// scale-to-zero sidecar needs to hibernate a branch
func ScaleToZeroClusterRoleRules(branchName string) []rbacv1.PolicyRule {
	return []rbacv1.PolicyRule{
		{
			APIGroups:     []string{v1alpha1.GroupVersion.Group},
			Resources:     []string{"branches"},
			ResourceNames: []string{branchName},
			Verbs:         []string{"get", "patch"},
		},
	}
}

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

// BackupCredentials references the Secret holding static S3 credentials. It is
// used when backing up to a non-AWS, S3-compatible endpoint (Cloudflare R2 in
// production, or RustFS for local development), where there is no IAM role to
// inherit. The Secret is managed outside this operator (e.g. SOPS in infra-new).
type BackupCredentials struct {
	SecretName         string
	AccessKeyIDKey     string
	SecretAccessKeyKey string
}

// ObjectStoreSpec defines the ObjectStoreSpec for a branch's backup storage.
// It configures cloud-provider object storage for CNPG backup retention.
//
// When backupsEndpoint is set the spec uses the AWS-S3 path with static
// credentials from the configured Secret (Cloudflare R2 in production, RustFS
// for local dev) and cloudProvider is ignored. Otherwise cloudProvider
// selects the credential shape: "gcp" wires barman to GKE workload identity
// federation; "aws" wires barman to inherit the pod's IAM role.
func ObjectStoreSpec(
	backupsBucket,
	backupsEndpoint,
	barmanRegionSecretName,
	barmanRegionSecretKey,
	retention,
	cloudProvider string,
	credentials BackupCredentials,
) barmanPluginApi.ObjectStoreSpec {
	awsCredentials := &apiv1.S3Credentials{
		InheritFromIAMRole: true,
		RegionReference: &apiv1.SecretKeySelector{
			Name: barmanRegionSecretName,
			Key:  barmanRegionSecretKey,
		},
	}

	spec := barmanPluginApi.ObjectStoreSpec{
		RetentionPolicy: retention,
		Configuration: apiv1.BarmanObjectStoreConfiguration{
			DestinationPath:   backupsBucket,
			BarmanCredentials: apiv1.BarmanCredentials{AWS: awsCredentials},
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

	// A custom endpoint means a non-AWS, S3-compatible store (e.g. Cloudflare R2
	// or RustFS for local dev). There's no IAM role to inherit, so switch to
	// static credentials from the configured Secret. The region reference is
	// kept: R2 expects a region ("auto") and RustFS ignores it.
	if backupsEndpoint != "" {
		spec.Configuration.EndpointURL = backupsEndpoint
		awsCredentials.InheritFromIAMRole = false
		awsCredentials.AccessKeyIDReference = &apiv1.SecretKeySelector{
			Name: credentials.SecretName,
			Key:  credentials.AccessKeyIDKey,
		}
		awsCredentials.SecretAccessKeyReference = &apiv1.SecretKeySelector{
			Name: credentials.SecretName,
			Key:  credentials.SecretAccessKeyKey,
		}
		return spec
	}

	// GKE workload identity federation: barman authenticates as the pod's GSA
	// directly, no static credentials needed.
	if cloudProvider == "gcp" {
		spec.Configuration.BarmanCredentials = apiv1.BarmanCredentials{
			Google: &apiv1.GoogleCredentials{GKEEnvironment: true},
		}
	}

	return spec
}

// ScheduledBackupSpec generates the ScheduledBackup spec apply configuration for a
// branch's scheduled backups. The method parameter determines the backup method:
// barman uses the barman-cloud plugin, pgbackrest uses in-core pgbackrest with
// full backups.
func ScheduledBackupSpec(
	clusterName, schedule string,
	suspend bool,
	method v1alpha1.BackupMethod,
) *apiv1ac.ScheduledBackupSpecApplyConfiguration {
	spec := apiv1ac.ScheduledBackupSpec().
		WithCluster(corev1ac.LocalObjectReference().WithName(clusterName)).
		WithSchedule(schedule).
		WithImmediate(true).
		WithSuspend(suspend)

	if method == v1alpha1.BackupMethodPgBackRest {
		return spec.
			WithMethod(apiv1.BackupMethodPgBackRest).
			WithPgBackRestBackupType(apiv1.PgBackRestBackupTypeFull)
	}

	return spec.
		WithMethod(apiv1.BackupMethodPlugin).
		WithPluginConfiguration(apiv1ac.BackupPluginConfiguration().
			WithName("barman-cloud.cloudnative-pg.io"))
}

// Secret builds a BasicAuth Secret with the given name, username, and password.
// Owner references and labels are set by the reconciler.
func Secret(name, namespace, username, password string) *v1.Secret {
	return &v1.Secret{
		Name:      name,
		Namespace: namespace,
		Type:      v1.SecretTypeBasicAuth,
		Data: map[string][]byte{
			v1.BasicAuthUsernameKey: []byte(username),
			v1.BasicAuthPasswordKey: []byte(password),
		},
	}
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
