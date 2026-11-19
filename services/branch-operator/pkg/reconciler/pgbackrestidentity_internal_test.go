package reconciler

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	"go.opentelemetry.io/otel/metric/noop"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"xata/internal/pgbackrest"
	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/gcp/gcsfolder"
	"xata/services/branch-operator/pkg/reconciler/metrics"
)

func TestReconcilePgBackRestToken(t *testing.T) {
	t.Parallel()

	sa := &corev1.ServiceAccount{
		Name: "branch-pgbackrest", Namespace: "xata-clusters", UID: types.UID("sa-uid"),
	}
	request := pgBackRestTokenRequestKey(sa, &authenticationv1.TokenRequest{
		Spec: authenticationv1.TokenRequestSpec{
			Audiences:         []string{pgBackRestAWSTokenAudience},
			ExpirationSeconds: new(int64(pgBackRestTokenLifetime.Seconds())),
		},
	})
	now := time.Now()
	stamp := func(t time.Time) string { return t.UTC().Format(time.RFC3339) }

	testcases := map[string]struct {
		annotations  map[string]string
		requestErr   error
		wantErr      bool
		wantRequests int
		wantToken    string
		wantNextMin  time.Duration
		wantNextMax  time.Duration
	}{
		"requests a token when the Secret has none": {
			wantRequests: 1,
			wantToken:    "new",
			wantNextMin:  pgBackRestTokenLifetime*8/10 - pgBackRestTokenRefreshJitter - time.Minute,
			wantNextMax:  pgBackRestTokenLifetime * 8 / 10,
		},
		"keeps the token before the refresh time": {
			annotations: map[string]string{
				PgBackRestTokenRequestAnnotation:    request,
				PgBackRestTokenExpirationAnnotation: stamp(now.Add(5 * time.Hour)),
				PgBackRestTokenRefreshAnnotation:    stamp(now.Add(time.Hour)),
			},
			wantToken:   "old",
			wantNextMin: time.Hour - time.Minute,
			wantNextMax: time.Hour,
		},
		"requests a token for a different request": {
			annotations: map[string]string{
				PgBackRestTokenRequestAnnotation:    "serviceAccount=other",
				PgBackRestTokenExpirationAnnotation: stamp(now.Add(5 * time.Hour)),
				PgBackRestTokenRefreshAnnotation:    stamp(now.Add(time.Hour)),
			},
			wantRequests: 1,
			wantToken:    "new",
			wantNextMin:  pgBackRestTokenLifetime*8/10 - pgBackRestTokenRefreshJitter - time.Minute,
			wantNextMax:  pgBackRestTokenLifetime * 8 / 10,
		},
		"keeps a valid token when the refresh fails": {
			annotations: map[string]string{
				PgBackRestTokenRequestAnnotation:    request,
				PgBackRestTokenExpirationAnnotation: stamp(now.Add(5 * time.Hour)),
				PgBackRestTokenRefreshAnnotation:    stamp(now.Add(-time.Minute)),
			},
			requestErr:   errors.New("unavailable"),
			wantRequests: 1,
			wantToken:    "old",
			wantNextMin:  pgBackRestTokenRetryDelay - time.Minute,
			wantNextMax:  2 * pgBackRestTokenRetryDelay,
		},
		"retries no later than the expiration": {
			annotations: map[string]string{
				PgBackRestTokenRequestAnnotation:    request,
				PgBackRestTokenExpirationAnnotation: stamp(now.Add(2 * time.Minute)),
				PgBackRestTokenRefreshAnnotation:    stamp(now.Add(-time.Hour)),
			},
			requestErr:   errors.New("unavailable"),
			wantRequests: 1,
			wantToken:    "old",
			wantNextMin:  time.Second,
			wantNextMax:  2 * time.Minute,
		},
		"fails when the refresh fails and the token expired": {
			annotations: map[string]string{
				PgBackRestTokenRequestAnnotation:    request,
				PgBackRestTokenExpirationAnnotation: stamp(now.Add(-time.Minute)),
				PgBackRestTokenRefreshAnnotation:    stamp(now.Add(-time.Hour)),
			},
			requestErr:   errors.New("unavailable"),
			wantRequests: 1,
			wantErr:      true,
			wantToken:    "old",
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			scheme := runtime.NewScheme()
			require.NoError(t, corev1.AddToScheme(scheme))
			require.NoError(t, v1alpha1.AddToScheme(scheme))
			require.NoError(t, apiv1.AddToScheme(scheme))

			branch := &v1alpha1.Branch{Name: "branch", UID: types.UID("branch-uid")}
			secret := &corev1.Secret{
				Name: "cluster-pgbackrest-web-identity", Namespace: "xata-clusters",
				Annotations: tc.annotations,
			}
			if tc.annotations != nil {
				secret.Data = map[string][]byte{pgbackrest.TokenKey: []byte("old")}
			}

			// The fake client writes to the objects it is given
			sa := sa.DeepCopy()
			requests := 0
			cluster := &apiv1.Cluster{
				Name: "cluster", Namespace: "xata-clusters", UID: types.UID("cluster-uid"),
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(branch, cluster, secret, sa).
				WithInterceptorFuncs(interceptor.Funcs{
					SubResourceCreate: func(_ context.Context, _ client.Client, _ string, _ client.Object, sub client.Object, _ ...client.SubResourceCreateOption) error {
						requests++
						if tc.requestErr != nil {
							return tc.requestErr
						}
						tr, ok := sub.(*authenticationv1.TokenRequest)
						if !ok {
							return fmt.Errorf("unexpected subresource %T", sub)
						}
						tr.Status.Token = "new"
						tr.Status.ExpirationTimestamp = metav1.NewTime(time.Now().Add(time.Duration(*tr.Spec.ExpirationSeconds) * time.Second))
						return nil
					},
				}).Build()
			r := &BranchReconciler{Client: c, Scheme: scheme, ClustersNamespace: "xata-clusters"}

			got, err := r.reconcilePgBackRestToken(ctx, branch, cluster, sa)
			require.Equal(t, tc.wantRequests, requests)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.GreaterOrEqual(t, got, tc.wantNextMin)
				require.LessOrEqual(t, got, tc.wantNextMax)
			}

			stored := &corev1.Secret{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(secret), stored))
			require.Equal(t, tc.wantToken, string(stored.Data[pgbackrest.TokenKey]))
		})
	}
}

func TestUsesPgBackRestBranchIdentity(t *testing.T) {
	t.Parallel()

	s3Backup := func(endpoint string) *v1alpha1.BackupSpec {
		return &v1alpha1.BackupSpec{
			Method: v1alpha1.BackupMethodPgBackRest,
			PgBackRest: &v1alpha1.PgBackRestSpec{
				S3: &v1alpha1.PgBackRestS3Spec{
					Bucket:   "some-bucket",
					Region:   "us-east-1",
					Endpoint: endpoint,
				},
			},
		}
	}

	gcsBackup := &v1alpha1.BackupSpec{
		Method: v1alpha1.BackupMethodPgBackRest,
		PgBackRest: &v1alpha1.PgBackRestSpec{
			GCS: &v1alpha1.PgBackRestGCSSpec{
				Bucket:              "some-bucket",
				ServiceAccountEmail: "cnpg@project.iam.gserviceaccount.com",
			},
		},
	}

	testcases := map[string]struct {
		cloudProvider string
		endpoint      string
		roleARN       string
		pool          string
		backup        *v1alpha1.BackupSpec
		want          bool
	}{
		"AWS IAM backups with a role": {
			cloudProvider: "aws",
			roleARN:       "arn:aws:iam::123456789012:role/test-cnpg-backups",
			backup:        s3Backup(""),
			want:          true,
		},
		"AWS IAM backups without a role": {
			cloudProvider: "aws",
			backup:        s3Backup(""),
			want:          true,
		},
		"GCP cell with S3 backups": {
			cloudProvider: "gcp",
			pool:          "project.svc.id.goog",
			backup:        s3Backup(""),
		},
		"GCP cell with GCS backups": {
			cloudProvider: "gcp",
			pool:          "project.svc.id.goog",
			backup:        gcsBackup,
			want:          true,
		},
		"GCP cell without a Workload Identity pool": {
			cloudProvider: "gcp",
			backup:        gcsBackup,
		},
		"AWS cell with GCS backups": {
			cloudProvider: "aws",
			pool:          "project.svc.id.goog",
			backup:        gcsBackup,
		},
		"cell with an S3-compatible endpoint": {
			cloudProvider: "aws",
			endpoint:      "https://rustfs.local",
			backup:        s3Backup(""),
		},
		"Branch with static keys": {
			cloudProvider: "aws",
			backup:        s3Backup("https://example.r2.cloudflarestorage.com"),
		},
		"Branch without backups": {
			cloudProvider: "aws",
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			r := &BranchReconciler{
				CloudProvider:                  tc.cloudProvider,
				BackupsEndpoint:                tc.endpoint,
				PgBackRestWebIdentityRoleARN:   tc.roleARN,
				PgBackRestWorkloadIdentityPool: tc.pool,
			}
			branch := &v1alpha1.Branch{Spec: v1alpha1.BranchSpec{BackupSpec: tc.backup}}

			got := r.usesPgBackRestBranchIdentity(branch)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestReconcilePgBackRestIdentityGCP(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, v1alpha1.AddToScheme(scheme))
	require.NoError(t, apiv1.AddToScheme(scheme))

	branch := &v1alpha1.Branch{
		Name: "branch", UID: types.UID("branch-uid"),
		Spec: v1alpha1.BranchSpec{
			ClusterSpec: v1alpha1.ClusterSpec{Name: new("cluster")},
			BackupSpec: &v1alpha1.BackupSpec{
				Method: v1alpha1.BackupMethodPgBackRest,
				PgBackRest: &v1alpha1.PgBackRestSpec{
					GCS: &v1alpha1.PgBackRestGCSSpec{
						Bucket:              "some-bucket",
						ServiceAccountEmail: "cnpg@project.iam.gserviceaccount.com",
					},
				},
			},
		},
	}
	cluster := &apiv1.Cluster{
		Name: "cluster", Namespace: "xata-clusters", UID: types.UID("cluster-uid"),
	}

	const issuer = "https://container.googleapis.com/v1/projects/project/locations/region/clusters/cell"
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{Issuer: issuer}).
		SignedString([]byte("key"))
	require.NoError(t, err)

	var audiences []string
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(branch, cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceCreate: func(_ context.Context, _ client.Client, _ string, _ client.Object, sub client.Object, _ ...client.SubResourceCreateOption) error {
				tr, ok := sub.(*authenticationv1.TokenRequest)
				if !ok {
					return fmt.Errorf("unexpected subresource %T", sub)
				}
				audiences = tr.Spec.Audiences
				tr.Status.Token = token
				tr.Status.ExpirationTimestamp = metav1.NewTime(time.Now().Add(time.Hour))
				return nil
			},
		}).Build()
	r := &BranchReconciler{
		Client: c, Scheme: scheme, ClustersNamespace: "xata-clusters",
		CloudProvider:                  "gcp",
		PgBackRestWorkloadIdentityPool: "project.svc.id.goog",
	}

	_, err = r.reconcilePgBackRestIdentity(ctx, branch)
	require.NoError(t, err)
	require.Equal(t, []string{"project.svc.id.goog"}, audiences)

	sa := &corev1.ServiceAccount{}
	saKey := client.ObjectKey{Name: "branch-pgbackrest", Namespace: "xata-clusters"}
	require.NoError(t, c.Get(ctx, saKey, sa))
	require.NotContains(t, sa.Annotations, pgbackrest.GCSFolderAnnotation)

	secret := &corev1.Secret{}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Name: "cluster-pgbackrest-web-identity", Namespace: "xata-clusters"}, secret))
	require.Equal(t, token, string(secret.Data[pgbackrest.TokenKey]))
	require.JSONEq(t, `{
		"type": "external_account",
		"audience": "identitynamespace:project.svc.id.goog:`+issuer+`",
		"subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
		"token_url": "https://sts.googleapis.com/v1/token",
		"credential_source": {"file": "/projected/pgbackrest/token", "format": {"type": "text"}}
	}`, string(secret.Data[pgbackrest.GCSCredentialsKey]))

	branch.Spec.BackupSpec.PgBackRest.GCS.KeyType = pgbackrest.KeyTypeWebID
	_, err = r.reconcilePgBackRestIdentity(ctx, branch)
	require.NoError(t, err)

	require.NoError(t, c.Get(ctx, saKey, sa))
	require.Equal(t, "gs://some-bucket/branch/", sa.Annotations[pgbackrest.GCSFolderAnnotation])
	for name, spec := range map[string]struct{ repoPath, stanza, want string }{
		"explicit path":   {"team/repository", "branch-2", "team/repository"},
		"stanza fallback": {"", "branch-2", "branch-2"},
		"branch fallback": {"", "", "branch"},
	} {
		t.Run(name, func(t *testing.T) {
			branch.Spec.BackupSpec.PgBackRest.RepoPath = spec.repoPath
			branch.Spec.BackupSpec.PgBackRest.StanzaName = spec.stanza
			require.NoError(t, c.Update(ctx, sa))
			_, err := r.reconcilePgBackRestServiceAccount(ctx, branch)
			require.NoError(t, err)
			require.NoError(t, c.Get(ctx, saKey, sa))
			require.Equal(t, "gs://some-bucket/"+spec.want+"/", sa.Annotations[pgbackrest.GCSFolderAnnotation])
		})
	}
	t.Run("migrates the account without an assigned cluster", func(t *testing.T) {
		branch.Spec.ClusterSpec.Name = nil
		delete(sa.Annotations, pgbackrest.GCSFolderAnnotation)
		require.NoError(t, c.Update(ctx, sa))
		refreshIn, err := r.reconcilePgBackRestIdentity(ctx, branch)
		require.NoError(t, err)
		require.Zero(t, refreshIn)
		require.NoError(t, c.Get(ctx, saKey, sa))
		require.Contains(t, sa.Annotations, pgbackrest.GCSFolderAnnotation)
	})
}

func TestSetGCSCredentials(t *testing.T) {
	t.Parallel()

	signed := func(claims jwt.RegisteredClaims) string {
		token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte("key"))
		require.NoError(t, err)
		return token
	}
	token := signed(jwt.RegisteredClaims{Issuer: "https://issuer"})

	testcases := map[string]struct {
		cloudProvider string
		pool          string
		token         string
		want          bool
		wantErr       string
	}{
		"GCP cell with a Workload Identity pool": {
			cloudProvider: "gcp",
			pool:          "project.svc.id.goog",
			token:         token,
			want:          true,
		},
		"GCP cell without a Workload Identity pool": {
			cloudProvider: "gcp",
			token:         token,
		},
		"AWS cell": {
			cloudProvider: "aws",
			pool:          "project.svc.id.goog",
			token:         token,
		},
		"token that is not a JWT": {
			cloudProvider: "gcp",
			pool:          "project.svc.id.goog",
			token:         "new",
			wantErr:       "read token issuer",
		},
		"token without an issuer": {
			cloudProvider: "gcp",
			pool:          "project.svc.id.goog",
			token:         signed(jwt.RegisteredClaims{}),
			wantErr:       "token has no issuer",
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			r := &BranchReconciler{
				CloudProvider:                  tc.cloudProvider,
				PgBackRestWorkloadIdentityPool: tc.pool,
			}
			secret := &corev1.Secret{Data: map[string][]byte{pgbackrest.TokenKey: []byte(tc.token)}}

			err := r.setGCSCredentials(secret)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)

			_, got := secret.Data[pgbackrest.GCSCredentialsKey]
			require.Equal(t, tc.want, got)
		})
	}
}

func TestReconcileRestoreSourceAccess(t *testing.T) {
	t.Parallel()

	const (
		sourceFolder = "gs://some-bucket/source/repo/"
		pool         = "project.svc.id.goog"
	)
	saKey := client.ObjectKey{Name: "branch-pgbackrest", Namespace: "xata-clusters"}
	clusterKey := client.ObjectKey{Name: "cluster", Namespace: "xata-clusters"}

	gcsBackup := func(keyType string) *v1alpha1.BackupSpec {
		return &v1alpha1.BackupSpec{
			Method: v1alpha1.BackupMethodPgBackRest,
			PgBackRest: &v1alpha1.PgBackRestSpec{
				GCS: &v1alpha1.PgBackRestGCSSpec{Bucket: "some-bucket", KeyType: keyType},
			},
		}
	}
	gcsRestoreFrom := func(bucket, keyType string) *v1alpha1.RestoreSpec {
		return &v1alpha1.RestoreSpec{
			Type: v1alpha1.RestoreTypeObjectStore,
			Name: "source",
			PgBackRest: &v1alpha1.RestorePgBackRestSpec{
				RepoPath:   "source/repo",
				StanzaName: "source",
				GCS:        &v1alpha1.PgBackRestGCSSpec{Bucket: bucket, KeyType: keyType},
			},
		}
	}
	gcsRestore := func(keyType string) *v1alpha1.RestoreSpec {
		return gcsRestoreFrom("some-bucket", keyType)
	}
	newBranch := func(backup *v1alpha1.BackupSpec, restore *v1alpha1.RestoreSpec) *v1alpha1.Branch {
		return &v1alpha1.Branch{
			Name: "branch", UID: types.UID("branch-uid"),
			Spec: v1alpha1.BranchSpec{
				ClusterSpec: v1alpha1.ClusterSpec{
					Name: new("cluster"), Image: "image", Instances: 1,
					Storage: v1alpha1.StorageSpec{Size: "1Gi"},
				},
				BackupSpec: backup,
				Restore:    restore,
			},
		}
	}
	setup := func(t *testing.T, cloudProvider, pool string, objs ...client.Object) (*BranchReconciler, client.Client) {
		t.Helper()
		scheme := runtime.NewScheme()
		require.NoError(t, corev1.AddToScheme(scheme))
		require.NoError(t, v1alpha1.AddToScheme(scheme))
		require.NoError(t, apiv1.AddToScheme(scheme))
		require.NoError(t, networkingv1.AddToScheme(scheme))
		require.NoError(t, rbacv1.AddToScheme(scheme))
		require.NoError(t, snapshotv1.AddToScheme(scheme))

		token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{Issuer: "https://issuer"}).
			SignedString([]byte("key"))
		require.NoError(t, err)
		m, err := metrics.New(noop.NewMeterProvider().Meter("test"))
		require.NoError(t, err)

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
			WithStatusSubresource(&v1alpha1.Branch{}).
			WithIndex(&apiv1.Cluster{}, ClusterOwnerKey, func(client.Object) []string { return nil }).
			WithInterceptorFuncs(interceptor.Funcs{
				SubResourceCreate: func(_ context.Context, _ client.Client, _ string, _ client.Object, sub client.Object, _ ...client.SubResourceCreateOption) error {
					tr, ok := sub.(*authenticationv1.TokenRequest)
					if !ok {
						return fmt.Errorf("unexpected subresource %T", sub)
					}
					tr.Status.Token = token
					tr.Status.ExpirationTimestamp = metav1.NewTime(time.Now().Add(time.Hour))
					return nil
				},
			}).Build()
		return &BranchReconciler{
			Client: c, Scheme: scheme, Metrics: m, ClustersNamespace: "xata-clusters",
			CloudProvider: cloudProvider, PgBackRestWorkloadIdentityPool: pool,
			PgBackRestGCSProjectNumber: "123456",
		}, c
	}
	reconcile := func(t *testing.T, r *BranchReconciler) {
		t.Helper()
		_, err := r.Reconcile(t.Context(), ctrl.Request{Name: "branch"})
		require.NoError(t, err)
	}
	getConditionOfType := func(t *testing.T, c client.Client, conditionType string) *metav1.Condition {
		t.Helper()
		br := &v1alpha1.Branch{}
		require.NoError(t, c.Get(t.Context(), client.ObjectKey{Name: "branch"}, br))
		return meta.FindStatusCondition(br.Status.Conditions, conditionType)
	}
	getCondition := func(t *testing.T, c client.Client) *metav1.Condition {
		t.Helper()
		return getConditionOfType(t, c, v1alpha1.RestoreSourceAccessConditionType)
	}
	patchSA := func(t *testing.T, c client.Client, key, value string) {
		t.Helper()
		sa := &corev1.ServiceAccount{}
		require.NoError(t, c.Get(t.Context(), saKey, sa))
		if value == "" {
			delete(sa.Annotations, key)
		} else {
			sa.Annotations[key] = value
		}
		require.NoError(t, c.Update(t.Context(), sa))
	}
	getSA := func(t *testing.T, c client.Client) *corev1.ServiceAccount {
		t.Helper()
		sa := &corev1.ServiceAccount{}
		require.NoError(t, c.Get(t.Context(), saKey, sa))
		return sa
	}

	t.Run("grants before the Cluster and releases after the first primary", func(t *testing.T) {
		t.Parallel()
		r, c := setup(t, "gcp", pool, newBranch(gcsBackup(pgbackrest.KeyTypeWebID), gcsRestore(pgbackrest.KeyTypeWebID)))

		reconcile(t, r)
		require.Equal(t, sourceFolder, getSA(t, c).Annotations[pgbackrest.GCSRestoreFolderAnnotation])
		require.True(t, apierrors.IsNotFound(c.Get(t.Context(), clusterKey, &apiv1.Cluster{})))
		require.Nil(t, getCondition(t, c))
		ready := getConditionOfType(t, c, v1alpha1.BranchReadyConditionType)
		require.NotNil(t, ready)
		require.Equal(t, metav1.ConditionFalse, ready.Status)
		require.Equal(t, v1alpha1.AwaitingRestoreSourceAccessReason, ready.Reason)
		require.Contains(t, ready.Message, sourceFolder)

		// gcsfolder observes the grant
		patchSA(t, c, gcsfolder.ObservedRestoreFolderAnnotation, sourceFolder)
		reconcile(t, r)
		cluster := &apiv1.Cluster{}
		require.NoError(t, c.Get(t.Context(), clusterKey, cluster))
		cond := getCondition(t, c)
		require.NotNil(t, cond)
		require.Equal(t, metav1.ConditionTrue, cond.Status)
		require.Equal(t, v1alpha1.RestoreSourceAccessGrantedReason, cond.Reason)
		require.Equal(t, sourceFolder, getSA(t, c).Annotations[pgbackrest.GCSRestoreFolderAnnotation])
		ready = getConditionOfType(t, c, v1alpha1.BranchReadyConditionType)
		require.Equal(t, v1alpha1.ResourcesReadyReason, ready.Reason)

		// The first instance pod marks itself as primary
		cluster.Status.CurrentPrimary = "cluster-1"
		require.NoError(t, c.Update(t.Context(), cluster))
		reconcile(t, r)
		cond = getCondition(t, c)
		require.NotNil(t, cond)
		require.Equal(t, metav1.ConditionFalse, cond.Status)
		require.Equal(t, v1alpha1.RestoreSourceAccessReleasedReason, cond.Reason)
		require.NotContains(t, getSA(t, c).Annotations, pgbackrest.GCSRestoreFolderAnnotation)

		// gcsfolder revokes; later reconciles never request the grant again
		patchSA(t, c, gcsfolder.ObservedRestoreFolderAnnotation, "")
		reconcile(t, r)
		require.NotContains(t, getSA(t, c).Annotations, pgbackrest.GCSRestoreFolderAnnotation)
		require.Equal(t, v1alpha1.RestoreSourceAccessReleasedReason, getCondition(t, c).Reason)
	})

	t.Run("Branch with a primary is released without a grant", func(t *testing.T) {
		t.Parallel()
		cluster := &apiv1.Cluster{
			Name: "cluster", Namespace: "xata-clusters",
			Status: apiv1.ClusterStatus{CurrentPrimary: "cluster-1"},
		}
		r, c := setup(t, "gcp", pool, newBranch(gcsBackup(pgbackrest.KeyTypeWebID), gcsRestore(pgbackrest.KeyTypeWebID)), cluster)

		reconcile(t, r)
		cond := getCondition(t, c)
		require.NotNil(t, cond)
		require.Equal(t, v1alpha1.RestoreSourceAccessReleasedReason, cond.Reason)
		require.NotContains(t, getSA(t, c).Annotations, pgbackrest.GCSRestoreFolderAnnotation)
	})

	t.Run("release keeps a request until it is observed", func(t *testing.T) {
		t.Parallel()
		branch := newBranch(gcsBackup(pgbackrest.KeyTypeWebID), gcsRestore(pgbackrest.KeyTypeWebID))
		branch.Status.Conditions = []metav1.Condition{{
			Type: v1alpha1.RestoreSourceAccessConditionType, Status: metav1.ConditionFalse,
			Reason: v1alpha1.RestoreSourceAccessReleasedReason,
		}}
		sa := &corev1.ServiceAccount{
			Name: saKey.Name, Namespace: saKey.Namespace,
			Annotations: map[string]string{pgbackrest.GCSRestoreFolderAnnotation: sourceFolder},
		}
		r, c := setup(t, "gcp", pool, branch, sa)

		_, err := r.reconcilePgBackRestServiceAccount(t.Context(), branch)
		require.NoError(t, err)
		require.Equal(t, sourceFolder, getSA(t, c).Annotations[pgbackrest.GCSRestoreFolderAnnotation])

		patchSA(t, c, gcsfolder.ObservedRestoreFolderAnnotation, sourceFolder)
		_, err = r.reconcilePgBackRestServiceAccount(t.Context(), branch)
		require.NoError(t, err)
		require.NotContains(t, getSA(t, c).Annotations, pgbackrest.GCSRestoreFolderAnnotation)
	})

	unchanged := map[string]struct {
		cloudProvider   string
		pool            string
		noProjectNumber bool
		branch          *v1alpha1.Branch
	}{
		"GCP cell without a gcsfolder reconciler": {
			cloudProvider: "gcp", pool: pool, noProjectNumber: true,
			branch: newBranch(gcsBackup(pgbackrest.KeyTypeWebID), gcsRestore(pgbackrest.KeyTypeWebID)),
		},
		"AWS cell": {
			cloudProvider: "aws",
			branch:        newBranch(gcsBackup(pgbackrest.KeyTypeWebID), gcsRestore(pgbackrest.KeyTypeWebID)),
		},
		"GCP cell without a Workload Identity pool": {
			cloudProvider: "gcp",
			branch:        newBranch(gcsBackup(pgbackrest.KeyTypeWebID), gcsRestore(pgbackrest.KeyTypeWebID)),
		},
		"source in another bucket": {
			cloudProvider: "gcp", pool: pool,
			branch: newBranch(gcsBackup(pgbackrest.KeyTypeWebID), gcsRestoreFrom("other-bucket", pgbackrest.KeyTypeWebID)),
		},
		"source without web identity": {
			cloudProvider: "gcp", pool: pool,
			branch: newBranch(gcsBackup(pgbackrest.KeyTypeWebID), gcsRestore("auto")),
		},
		"Branch without web identity": {
			cloudProvider: "gcp", pool: pool,
			branch: newBranch(gcsBackup("auto"), gcsRestore(pgbackrest.KeyTypeWebID)),
		},
		"Branch without a restore": {
			cloudProvider: "gcp", pool: pool,
			branch: newBranch(gcsBackup(pgbackrest.KeyTypeWebID), nil),
		},
		"restore from the repository of the Branch": {
			cloudProvider: "gcp", pool: pool,
			branch: newBranch(gcsBackup(pgbackrest.KeyTypeWebID), &v1alpha1.RestoreSpec{
				Type: v1alpha1.RestoreTypeObjectStore, Name: "branch-1",
			}),
		},
	}
	for name, tc := range unchanged {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			r, c := setup(t, tc.cloudProvider, tc.pool, tc.branch)
			if tc.noProjectNumber {
				r.PgBackRestGCSProjectNumber = ""
			}

			reconcile(t, r)
			require.NoError(t, c.Get(t.Context(), clusterKey, &apiv1.Cluster{}))
			require.Nil(t, getCondition(t, c))
			sa := &corev1.ServiceAccount{}
			if err := c.Get(t.Context(), saKey, sa); !apierrors.IsNotFound(err) {
				require.NoError(t, err)
				require.NotContains(t, sa.Annotations, pgbackrest.GCSRestoreFolderAnnotation)
			}
		})
	}
}
