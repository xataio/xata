package resources

import (
	walgv1beta1 "github.com/wal-g/cnpg-plugin-wal-g/api/v1beta1"

	"xata/services/branching-replica-operator/api/v1alpha1"
)

// BackupConfigSpec generates the WAL-G plugin BackupConfig spec for a
// BranchingReplica
func BackupConfigSpec(replica *v1alpha1.BranchingReplica) walgv1beta1.BackupConfigSpec {
	s3 := replica.Spec.Source.S3

	spec := walgv1beta1.BackupConfigSpec{
		Storage: walgv1beta1.StorageConfig{
			StorageType: walgv1beta1.StorageTypeS3,
			S3: &walgv1beta1.S3StorageConfig{
				Prefix:             s3.Prefix,
				Region:             s3.Region,
				EndpointURL:        s3.EndpointURL,
				ForcePathStyle:     s3.ForcePathStyle,
				AccessKeyIDRef:     s3.AccessKeyIDRef.DeepCopy(),
				AccessKeySecretRef: s3.AccessKeySecretRef.DeepCopy(),
			},
		},
		Retention: walgv1beta1.BackupRetentionConfig{
			IgnoreForBackupConfigDeletion: true,
		},
	}

	if walg := replica.Spec.WALG; walg != nil {
		spec.DownloadConcurrency = walg.DownloadConcurrency
		spec.Resources = *walg.Resources.DeepCopy()
	}

	return spec
}
