package resources_test

import (
	"testing"

	"xata/services/branch-operator/pkg/reconciler/resources"

	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	apiv1ac "github.com/xataio/xata-cnpg/pkg/client/applyconfiguration/api/v1"
	v1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
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

func TestScheduledBackupSpec(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		name       string
		branchName string
		schedule   string
		suspend    bool
		want       *apiv1ac.ScheduledBackupSpecApplyConfiguration
	}{
		{
			name:       "scheduled backup",
			branchName: "test-branch-1",
			schedule:   "0 0 0 * * *",
			want: apiv1ac.ScheduledBackupSpec().
				WithCluster(corev1ac.LocalObjectReference().WithName("test-branch-1")).
				WithSchedule("0 0 0 * * *").
				WithImmediate(true).
				WithSuspend(false).
				WithMethod(apiv1.BackupMethodPgBackRest).
				WithPgBackRestBackupType(apiv1.PgBackRestBackupTypeFull),
		},
		{
			name:       "suspended scheduled backup",
			branchName: "test-branch-2",
			schedule:   "0 0 2 * * 0",
			suspend:    true,
			want: apiv1ac.ScheduledBackupSpec().
				WithCluster(corev1ac.LocalObjectReference().WithName("test-branch-2")).
				WithSchedule("0 0 2 * * 0").
				WithImmediate(true).
				WithSuspend(true).
				WithMethod(apiv1.BackupMethodPgBackRest).
				WithPgBackRestBackupType(apiv1.PgBackRestBackupTypeFull),
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			got := resources.ScheduledBackupSpec(tc.branchName, tc.schedule, tc.suspend)

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
				Name:      "branch-1-superuser",
				Namespace: "xata-clusters",
				Type:      v1.SecretTypeBasicAuth,
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
				Name:      "branch-1-app",
				Namespace: "xata-clusters",
				Type:      v1.SecretTypeBasicAuth,
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

func TestScaleToZeroClusterRoleRules(t *testing.T) {
	t.Parallel()

	testcases := []string{
		"branch-1",
		"branch-2",
	}

	for _, branchName := range testcases {
		t.Run(branchName, func(t *testing.T) {
			got := resources.ScaleToZeroClusterRoleRules(branchName)

			want := []rbacv1.PolicyRule{
				{
					APIGroups:     []string{"xata.io"},
					Resources:     []string{"branches"},
					ResourceNames: []string{branchName},
					Verbs:         []string{"get", "patch"},
				},
			}

			require.Equal(t, want, got)
		})
	}
}
