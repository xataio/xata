package resources_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	walgv1beta1 "github.com/wal-g/cnpg-plugin-wal-g/api/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"xata/services/branching-replica-operator/api/v1alpha1"
	"xata/services/branching-replica-operator/pkg/reconciler/resources"
)

func TestBackupConfigSpec(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		modifier func(*v1alpha1.BranchingReplica)
		want     walgv1beta1.BackupConfigSpec
	}{
		"minimal configuration": {
			modifier: nil,
			want: walgv1beta1.BackupConfigSpec{
				Storage: baseStorageConfig(),
				Retention: walgv1beta1.BackupRetentionConfig{
					IgnoreForBackupConfigDeletion: true,
				},
			},
		},
		"region": {
			modifier: func(replica *v1alpha1.BranchingReplica) {
				replica.Spec.Source.S3.Region = "us-east-1"
			},
			want: walgv1beta1.BackupConfigSpec{
				Storage: walgv1beta1.StorageConfig{
					StorageType: "s3",
					S3: &walgv1beta1.S3StorageConfig{
						Prefix: testPrefix,
						Region: "us-east-1",
						AccessKeyIDRef: &corev1.SecretKeySelector{
							Name: "test-credentials",
							Key:  "AWS_ACCESS_KEY_ID",
						},
						AccessKeySecretRef: &corev1.SecretKeySelector{
							Name: "test-credentials",
							Key:  "AWS_SECRET_ACCESS_KEY",
						},
					},
				},
				Retention: walgv1beta1.BackupRetentionConfig{
					IgnoreForBackupConfigDeletion: true,
				},
			},
		},
		"s3-compatible endpoint": {
			modifier: func(replica *v1alpha1.BranchingReplica) {
				replica.Spec.Source.S3.EndpointURL = "https://s3.example.com"
				replica.Spec.Source.S3.ForcePathStyle = true
			},
			want: walgv1beta1.BackupConfigSpec{
				Storage: walgv1beta1.StorageConfig{
					StorageType: "s3",
					S3: &walgv1beta1.S3StorageConfig{
						Prefix:         testPrefix,
						EndpointURL:    "https://s3.example.com",
						ForcePathStyle: true,
						AccessKeyIDRef: &corev1.SecretKeySelector{
							Name: "test-credentials",
							Key:  "AWS_ACCESS_KEY_ID",
						},
						AccessKeySecretRef: &corev1.SecretKeySelector{
							Name: "test-credentials",
							Key:  "AWS_SECRET_ACCESS_KEY",
						},
					},
				},
				Retention: walgv1beta1.BackupRetentionConfig{
					IgnoreForBackupConfigDeletion: true,
				},
			},
		},
		"sidecar download concurrency": {
			modifier: func(replica *v1alpha1.BranchingReplica) {
				replica.Spec.WALG = &v1alpha1.WALGSpec{
					DownloadConcurrency: new(4),
				}
			},
			want: walgv1beta1.BackupConfigSpec{
				Storage: baseStorageConfig(),
				Retention: walgv1beta1.BackupRetentionConfig{
					IgnoreForBackupConfigDeletion: true,
				},
				DownloadConcurrency: new(4),
			},
		},
		"sidecar resources": {
			modifier: func(replica *v1alpha1.BranchingReplica) {
				replica.Spec.WALG = &v1alpha1.WALGSpec{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("50m"),
							corev1.ResourceMemory: resource.MustParse("512Mi"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("500m"),
							corev1.ResourceMemory: resource.MustParse("512Mi"),
						},
					},
				}
			},
			want: walgv1beta1.BackupConfigSpec{
				Storage: baseStorageConfig(),
				Retention: walgv1beta1.BackupRetentionConfig{
					IgnoreForBackupConfigDeletion: true,
				},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("50m"),
						corev1.ResourceMemory: resource.MustParse("512Mi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("500m"),
						corev1.ResourceMemory: resource.MustParse("512Mi"),
					},
				},
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			replica := baseBranchingReplica()
			if tt.modifier != nil {
				tt.modifier(replica)
			}

			got := resources.BackupConfigSpec(replica)

			require.Equal(t, tt.want, got)
		})
	}
}

// baseBranchingReplica returns a BranchingReplica with only the required
// fields of its spec set
func baseBranchingReplica() *v1alpha1.BranchingReplica {
	return &v1alpha1.BranchingReplica{
		Name:      testReplicaName,
		Namespace: "xata-clusters",
		Spec:      baseClusterConfig().BranchingReplicaSpec,
	}
}

// baseStorageConfig returns the expected storage configuration for a default
// baseBranchingReplica()
func baseStorageConfig() walgv1beta1.StorageConfig {
	return walgv1beta1.StorageConfig{
		StorageType: "s3",
		S3: &walgv1beta1.S3StorageConfig{
			Prefix: testPrefix,
			AccessKeyIDRef: &corev1.SecretKeySelector{
				Name: "test-credentials",
				Key:  "AWS_ACCESS_KEY_ID",
			},
			AccessKeySecretRef: &corev1.SecretKeySelector{
				Name: "test-credentials",
				Key:  "AWS_SECRET_ACCESS_KEY",
			},
		},
	}
}
