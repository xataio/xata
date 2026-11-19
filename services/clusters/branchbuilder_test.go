package clusters

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	"xata/internal/pgbackrest"
	"xata/services/branch-operator/api/v1alpha1"
)

// webIdentityKeyType is the S3 key type of a new branch on AWS S3.
const webIdentityKeyType = pgbackrest.KeyTypeWebID

// webIdentityRepoPath is the repo path of a new branch on AWS S3, in the
// clusters namespace of the tests.
func webIdentityRepoPath(branchName string) string {
	return pgbackrest.RepoPath("xata-clusters", branchName)
}

func TestWithPgBackRestS3(t *testing.T) {
	t.Parallel()

	testcases := map[string]struct {
		provider     string
		endpoint     string
		wantKeyType  string
		wantRepoPath string
	}{
		"aws s3 uses the web identity token": {
			provider:     CloudProviderAWS,
			wantKeyType:  pgbackrest.KeyTypeWebID,
			wantRepoPath: "system:serviceaccount:xata-clusters:branch-id-pgbackrest",
		},
		"s3-compatible endpoint keeps the default repo": {
			provider: CloudProviderAWS,
			endpoint: "http://rustfs:9000",
		},
		"s3 outside aws keeps the default repo": {},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			b := &BranchBuilder{branch: &v1alpha1.Branch{
				Name: "branch-id",
				Spec: v1alpha1.BranchSpec{
					BackupSpec: &v1alpha1.BackupSpec{
						Method:     v1alpha1.BackupMethodPgBackRest,
						PgBackRest: &v1alpha1.PgBackRestSpec{},
					},
				},
			}}

			got := b.WithPgBackRest(tc.provider, "bucket", "us-east-1", tc.endpoint, "", "", "", "", "xata-clusters").
				Build().Spec.BackupSpec.PgBackRest

			require.Equal(t, tc.wantKeyType, got.S3.KeyType)
			require.Equal(t, tc.wantRepoPath, got.RepoPath)
		})
	}
}

func TestWithPgBackRestCipher(t *testing.T) {
	t.Parallel()

	testcases := map[string]struct {
		enabled bool
		method  v1alpha1.BackupMethod
		wantRef *corev1.SecretKeySelector
	}{
		"enabled encryption references the cipher secret": {
			enabled: true,
			method:  v1alpha1.BackupMethodPgBackRest,
			wantRef: &corev1.SecretKeySelector{
				Name: "branch-id-pgbackrest",
				Key:  pgbackrest.CipherPassphraseKey,
			},
		},
		"disabled encryption leaves the repository unencrypted": {
			method:  v1alpha1.BackupMethodPgBackRest,
			wantRef: nil,
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			b := &BranchBuilder{branch: &v1alpha1.Branch{
				Name: "branch-id",
				Spec: v1alpha1.BranchSpec{
					BackupSpec: &v1alpha1.BackupSpec{
						Method:     tc.method,
						PgBackRest: &v1alpha1.PgBackRestSpec{},
					},
				},
			}}

			got := b.WithPgBackRestCipher(tc.enabled).
				Build().Spec.BackupSpec.PgBackRest

			require.Equal(t, tc.wantRef, got.CipherPassphraseSecretRef)
		})
	}
}

func TestWithPgBackRestRestoreCipher(t *testing.T) {
	t.Parallel()

	// sourceBranch returns the restore source, encrypted or not
	sourceBranch := func(encrypted bool) *v1alpha1.Branch {
		source := &v1alpha1.Branch{
			Name: "source",
			Spec: v1alpha1.BranchSpec{
				BackupSpec: &v1alpha1.BackupSpec{
					Method:     v1alpha1.BackupMethodPgBackRest,
					PgBackRest: &v1alpha1.PgBackRestSpec{},
				},
			},
		}
		if encrypted {
			source.Spec.BackupSpec.PgBackRest.CipherPassphraseSecretRef = &corev1.SecretKeySelector{
				Name: "source-pgbackrest",
				Key:  pgbackrest.CipherPassphraseKey,
			}
		}
		return source
	}

	testcases := map[string]struct {
		source      *v1alpha1.Branch
		restoreType v1alpha1.RestoreType
		wantRef     *corev1.SecretKeySelector
	}{
		"encrypted source references the restore cipher": {
			source:      sourceBranch(true),
			restoreType: v1alpha1.RestoreTypeObjectStore,
			wantRef: &corev1.SecretKeySelector{
				Name: "branch-id-pgbackrest",
				Key:  pgbackrest.RestoreCipherPassphraseKey,
			},
		},
		"unencrypted source needs no restore cipher": {
			source:      sourceBranch(false),
			restoreType: v1alpha1.RestoreTypeObjectStore,
			wantRef:     nil,
		},
		"no source needs no restore cipher": {
			source:      nil,
			restoreType: v1alpha1.RestoreTypeObjectStore,
			wantRef:     nil,
		},
		"volume snapshot restore does not read the source repository": {
			source:      sourceBranch(true),
			restoreType: v1alpha1.RestoreTypeVolumeSnapshot,
			wantRef:     nil,
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			b := &BranchBuilder{branch: &v1alpha1.Branch{
				Name: "branch-id",
				Spec: v1alpha1.BranchSpec{
					BackupSpec: &v1alpha1.BackupSpec{
						Method:     v1alpha1.BackupMethodPgBackRest,
						PgBackRest: &v1alpha1.PgBackRestSpec{},
					},
					Restore: &v1alpha1.RestoreSpec{
						Type: tc.restoreType,
						Name: "source",
					},
				},
			}}

			got := b.WithPgBackRestRestoreCipher(tc.source).
				Build().Spec.Restore

			require.Equal(t, tc.wantRef, got.PgBackRestCipherPassphraseSecretRef)
		})
	}
}

func TestWithPgBackRestRestoreSource(t *testing.T) {
	t.Parallel()

	source := &v1alpha1.Branch{
		Name: "source",
		Spec: v1alpha1.BranchSpec{
			BackupSpec: &v1alpha1.BackupSpec{
				Method: v1alpha1.BackupMethodPgBackRest,
				PgBackRest: &v1alpha1.PgBackRestSpec{
					GCS: &v1alpha1.PgBackRestGCSSpec{Bucket: "source-bucket", KeyType: pgbackrest.KeyTypeWebID},
				},
			},
		},
	}

	testcases := map[string]struct {
		backup *v1alpha1.BackupSpec
		want   *v1alpha1.RestorePgBackRestSpec
	}{
		"pgbackrest branch reads the source repository": {
			backup: &v1alpha1.BackupSpec{
				Method:     v1alpha1.BackupMethodPgBackRest,
				PgBackRest: &v1alpha1.PgBackRestSpec{},
			},
			want: &v1alpha1.RestorePgBackRestSpec{
				RepoPath:   "source",
				StanzaName: "source",
				GCS:        &v1alpha1.PgBackRestGCSSpec{Bucket: "source-bucket", KeyType: pgbackrest.KeyTypeWebID},
			},
		},
		"branch without backups keeps the restore name": {
			backup: nil,
			want:   nil,
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			b := &BranchBuilder{branch: &v1alpha1.Branch{
				Name: "branch-id",
				Spec: v1alpha1.BranchSpec{
					BackupSpec: tc.backup,
					Restore: &v1alpha1.RestoreSpec{
						Type: v1alpha1.RestoreTypeObjectStore,
						Name: "source",
					},
				},
			}}

			got := b.WithPgBackRestRestoreSource(source).Build().Spec.Restore

			require.Equal(t, tc.want, got.PgBackRest)
		})
	}
}
