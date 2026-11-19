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
	apiv1ac "github.com/xataio/xata-cnpg/pkg/client/applyconfiguration/api/v1"
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

			branch := &v1alpha1.Branch{
				Name: "branch", UID: types.UID("branch-uid"),
				Spec: v1alpha1.BranchSpec{ClusterSpec: v1alpha1.ClusterSpec{Name: new("cluster")}},
			}
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

			got, err := r.reconcilePgBackRestToken(ctx, branch, cluster, sa, pgBackRestAWSTokenAudience, pgBackRestTokenLifetime)
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

func TestLendRestoreSourceToken(t *testing.T) {
	t.Parallel()

	const roleARN = "arn:aws:iam::123456789012:role/cell-cnpg-backups-restore"
	clusterKey := client.ObjectKey{Name: "cluster", Namespace: "xata-clusters"}
	secretKey := client.ObjectKey{Name: "cluster-pgbackrest-web-identity", Namespace: "xata-clusters"}
	lentRequest := "serviceAccount=source-pgbackrest,uid=source-uid,audiences=[sts.amazonaws.com/restore],expirationSeconds=3600"

	s3Backup := func(keyType string) *v1alpha1.BackupSpec {
		return &v1alpha1.BackupSpec{
			Method: v1alpha1.BackupMethodPgBackRest,
			PgBackRest: &v1alpha1.PgBackRestSpec{
				S3: &v1alpha1.PgBackRestS3Spec{Bucket: "some-bucket", Region: "us-east-1", KeyType: keyType},
			},
		}
	}
	s3RestoreFrom := func(s3 v1alpha1.PgBackRestS3Spec) *v1alpha1.RestoreSpec {
		return &v1alpha1.RestoreSpec{
			Type: v1alpha1.RestoreTypeObjectStore,
			Name: "source",
			PgBackRest: &v1alpha1.RestorePgBackRestSpec{
				RepoPath: "system:serviceaccount:xata-clusters:source-pgbackrest", StanzaName: "source", S3: &s3,
			},
		}
	}
	s3Restore := s3RestoreFrom(v1alpha1.PgBackRestS3Spec{
		Bucket: "some-bucket", Region: "us-east-1", KeyType: pgbackrest.KeyTypeWebID,
	})
	newBranch := func(backup *v1alpha1.BackupSpec, restore *v1alpha1.RestoreSpec) *v1alpha1.Branch {
		return &v1alpha1.Branch{
			Name: "branch", UID: types.UID("branch-uid"),
			Spec: v1alpha1.BranchSpec{
				ClusterSpec: v1alpha1.ClusterSpec{
					Name: new("cluster"), Image: "image", Instances: 1,
					Storage: v1alpha1.StorageSpec{Size: "1Gi"},
				},
				BackupSpec: backup,
				Restore:    restore.DeepCopy(),
			},
		}
	}
	sourceSA := func() *corev1.ServiceAccount {
		return &corev1.ServiceAccount{Name: "source-pgbackrest", Namespace: "xata-clusters", UID: types.UID("source-uid")}
	}

	// recorder holds the ServiceAccounts that got a token and the Secret at
	// the time the Cluster is applied. It fails the next failStatusWrites
	// status writes and the token requests for failTokenFor.
	type recorder struct {
		tokensFor        []string
		secretAtApply    *corev1.Secret
		clusterApplied   bool
		failStatusWrites int
		failTokenFor     string
	}
	setup := func(t *testing.T, cloudProvider, restoreRoleARN string, objs ...client.Object) (*BranchReconciler, client.Client, *recorder) {
		t.Helper()
		scheme := runtime.NewScheme()
		require.NoError(t, corev1.AddToScheme(scheme))
		require.NoError(t, v1alpha1.AddToScheme(scheme))
		require.NoError(t, apiv1.AddToScheme(scheme))
		require.NoError(t, networkingv1.AddToScheme(scheme))
		require.NoError(t, rbacv1.AddToScheme(scheme))
		require.NoError(t, snapshotv1.AddToScheme(scheme))
		m, err := metrics.New(noop.NewMeterProvider().Meter("test"))
		require.NoError(t, err)

		rec := &recorder{}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
			WithStatusSubresource(&v1alpha1.Branch{}).
			WithIndex(&apiv1.Cluster{}, ClusterOwnerKey, func(client.Object) []string { return nil }).
			WithInterceptorFuncs(interceptor.Funcs{
				Apply: func(ctx context.Context, cl client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
					if _, ok := obj.(*apiv1ac.ClusterApplyConfiguration); ok && !rec.clusterApplied {
						rec.clusterApplied = true
						secret := &corev1.Secret{}
						if err := cl.Get(ctx, secretKey, secret); err == nil {
							rec.secretAtApply = secret
						}
					}
					return cl.Apply(ctx, obj, opts...)
				},
				SubResourceApply: func(ctx context.Context, cl client.Client, sub string, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
					if rec.failStatusWrites > 0 {
						rec.failStatusWrites--
						return errors.New("status write failed")
					}
					return cl.SubResource(sub).Apply(ctx, obj, opts...)
				},
				SubResourceCreate: func(_ context.Context, _ client.Client, _ string, obj client.Object, sub client.Object, _ ...client.SubResourceCreateOption) error {
					tr, ok := sub.(*authenticationv1.TokenRequest)
					if !ok {
						return fmt.Errorf("unexpected subresource %T", sub)
					}
					if obj.GetName() == rec.failTokenFor {
						return errors.New("token request failed")
					}
					rec.tokensFor = append(rec.tokensFor, obj.GetName())
					tr.Status.Token = fmt.Sprintf("token-%d", len(rec.tokensFor))
					tr.Status.ExpirationTimestamp = metav1.NewTime(time.Now().Add(time.Duration(*tr.Spec.ExpirationSeconds) * time.Second))
					return nil
				},
			}).Build()
		return &BranchReconciler{
			Client: c, Scheme: scheme, Metrics: m, ClustersNamespace: "xata-clusters",
			CloudProvider: cloudProvider, PgBackRestS3RestoreRoleARN: restoreRoleARN,
			PgBackRestWebIdentityRoleARN: "arn:aws:iam::123456789012:role/cell-cnpg-backups",
		}, c, rec
	}
	reconcile := func(t *testing.T, r *BranchReconciler) ctrl.Result {
		t.Helper()
		res, err := r.Reconcile(t.Context(), ctrl.Request{Name: "branch"})
		require.NoError(t, err)
		return res
	}
	getCondition := func(t *testing.T, c client.Client, conditionType string) *metav1.Condition {
		t.Helper()
		br := &v1alpha1.Branch{}
		require.NoError(t, c.Get(t.Context(), client.ObjectKey{Name: "branch"}, br))
		return meta.FindStatusCondition(br.Status.Conditions, conditionType)
	}
	getSecret := func(t *testing.T, c client.Client) *corev1.Secret {
		t.Helper()
		secret := &corev1.Secret{}
		require.NoError(t, c.Get(t.Context(), secretKey, secret))
		return secret
	}
	requireNotReady := func(t *testing.T, c client.Client, reason string) *metav1.Condition {
		t.Helper()
		require.True(t, apierrors.IsNotFound(c.Get(t.Context(), clusterKey, &apiv1.Cluster{})))
		require.True(t, apierrors.IsNotFound(c.Get(t.Context(), secretKey, &corev1.Secret{})))
		require.Nil(t, getCondition(t, c, v1alpha1.RestoreSourceAccessConditionType))
		ready := getCondition(t, c, v1alpha1.BranchReadyConditionType)
		require.NotNil(t, ready)
		require.Equal(t, metav1.ConditionFalse, ready.Status)
		require.Equal(t, reason, ready.Reason)
		return ready
	}

	t.Run("lends the source token before the Cluster and swaps after the first primary", func(t *testing.T) {
		t.Parallel()
		r, c, rec := setup(t, "aws", roleARN, newBranch(s3Backup(pgbackrest.KeyTypeWebID), s3Restore), sourceSA())

		reconcile(t, r)
		require.NotNil(t, rec.secretAtApply, "Secret must exist before the Cluster")
		require.Equal(t, lentRequest, rec.secretAtApply.Annotations[PgBackRestTokenRequestAnnotation])
		require.Equal(t, "token-1", string(rec.secretAtApply.Data[pgbackrest.TokenKey]))
		require.Len(t, rec.secretAtApply.OwnerReferences, 1)
		require.Equal(t, v1alpha1.BranchKind, rec.secretAtApply.OwnerReferences[0].Kind)
		require.Equal(t, []string{"source-pgbackrest"}, rec.tokensFor)
		require.Equal(t, v1alpha1.RestoreSourceAccessGrantedReason,
			getCondition(t, c, v1alpha1.RestoreSourceAccessConditionType).Reason)

		cluster := &apiv1.Cluster{}
		require.NoError(t, c.Get(t.Context(), clusterKey, cluster))
		require.Len(t, cluster.Spec.ExternalClusters, 1)
		source := cluster.Spec.ExternalClusters[0].PgBackRest.Repository.S3
		require.Equal(t, roleARN, source.RoleARN)
		require.Equal(t, pgbackrest.KeyTypeWebID, source.KeyType)
		require.Empty(t, cluster.Spec.Backup.PgBackRest.Repository.S3.RoleARN)

		// The Cluster takes over the Secret from the Branch
		secret := getSecret(t, c)
		require.Len(t, secret.OwnerReferences, 1)
		require.Equal(t, "Cluster", secret.OwnerReferences[0].Kind)
		require.Equal(t, cluster.UID, secret.OwnerReferences[0].UID)
		require.Equal(t, lentRequest, secret.Annotations[PgBackRestTokenRequestAnnotation])
		require.Equal(t, []string{"source-pgbackrest"}, rec.tokensFor)

		// A long restore refreshes the lent token
		secret.Annotations[PgBackRestTokenRefreshAnnotation] = time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
		require.NoError(t, c.Update(t.Context(), secret))
		res := reconcile(t, r)
		require.Equal(t, []string{"source-pgbackrest", "source-pgbackrest"}, rec.tokensFor)
		require.LessOrEqual(t, res.RequeueAfter, pgBackRestRestoreTokenLifetime*8/10)
		secret = getSecret(t, c)
		require.Equal(t, lentRequest, secret.Annotations[PgBackRestTokenRequestAnnotation])
		require.Equal(t, "token-2", string(secret.Data[pgbackrest.TokenKey]))

		// The first instance pod marks itself as primary
		require.NoError(t, c.Get(t.Context(), clusterKey, cluster))
		cluster.Status.CurrentPrimary = "cluster-1"
		require.NoError(t, c.Update(t.Context(), cluster))
		reconcile(t, r)
		require.Equal(t, v1alpha1.RestoreSourceAccessReleasedReason,
			getCondition(t, c, v1alpha1.RestoreSourceAccessConditionType).Reason)
		require.Equal(t, []string{"source-pgbackrest", "source-pgbackrest", "branch-pgbackrest"}, rec.tokensFor)
		secret = getSecret(t, c)
		require.Equal(t, "token-3", string(secret.Data[pgbackrest.TokenKey]))
		require.Contains(t, secret.Annotations[PgBackRestTokenRequestAnnotation],
			"serviceAccount=branch-pgbackrest,")
		require.Contains(t, secret.Annotations[PgBackRestTokenRequestAnnotation],
			",audiences=[sts.amazonaws.com],expirationSeconds=86400")

		// Later reconciles keep the own token
		reconcile(t, r)
		require.Len(t, rec.tokensFor, 3)
	})

	// lend runs the first reconcile of a restore and returns the Cluster.
	lend := func(t *testing.T, r *BranchReconciler, c client.Client) *apiv1.Cluster {
		t.Helper()
		reconcile(t, r)
		require.Equal(t, lentRequest, getSecret(t, c).Annotations[PgBackRestTokenRequestAnnotation])
		cluster := &apiv1.Cluster{}
		require.NoError(t, c.Get(t.Context(), clusterKey, cluster))
		cluster.Status.CurrentPrimary = "cluster-1"
		require.NoError(t, c.Update(t.Context(), cluster))
		return cluster
	}

	t.Run("release is persisted before the own token", func(t *testing.T) {
		t.Parallel()
		r, c, rec := setup(t, "aws", roleARN, newBranch(s3Backup(pgbackrest.KeyTypeWebID), s3Restore), sourceSA())
		lend(t, r, c)

		rec.failStatusWrites = 1
		_, err := r.Reconcile(t.Context(), ctrl.Request{Name: "branch"})
		require.ErrorContains(t, err, "persist restore source release")
		require.Equal(t, lentRequest, getSecret(t, c).Annotations[PgBackRestTokenRequestAnnotation])
		require.Equal(t, []string{"source-pgbackrest"}, rec.tokensFor)

		reconcile(t, r)
		require.Equal(t, v1alpha1.RestoreSourceAccessReleasedReason,
			getCondition(t, c, v1alpha1.RestoreSourceAccessConditionType).Reason)
		require.Equal(t, []string{"source-pgbackrest", "branch-pgbackrest"}, rec.tokensFor)
		require.Contains(t, getSecret(t, c).Annotations[PgBackRestTokenRequestAnnotation],
			"serviceAccount=branch-pgbackrest,")
	})

	t.Run("failed own token after the release clears the lent token", func(t *testing.T) {
		t.Parallel()
		r, c, rec := setup(t, "aws", roleARN, newBranch(s3Backup(pgbackrest.KeyTypeWebID), s3Restore), sourceSA())
		lend(t, r, c)

		rec.failTokenFor = "branch-pgbackrest"
		_, err := r.Reconcile(t.Context(), ctrl.Request{Name: "branch"})
		require.ErrorContains(t, err, "token request failed")
		require.Equal(t, v1alpha1.RestoreSourceAccessReleasedReason,
			getCondition(t, c, v1alpha1.RestoreSourceAccessConditionType).Reason)
		secret := getSecret(t, c)
		require.NotContains(t, secret.Data, pgbackrest.TokenKey)
		require.NotContains(t, secret.Annotations, PgBackRestTokenRequestAnnotation)
		require.NotContains(t, secret.Annotations, PgBackRestTokenExpirationAnnotation)
		require.NotContains(t, secret.Annotations, PgBackRestTokenRefreshAnnotation)

		rec.failTokenFor = ""
		reconcile(t, r)
		secret = getSecret(t, c)
		require.Contains(t, secret.Annotations[PgBackRestTokenRequestAnnotation], "serviceAccount=branch-pgbackrest,")
		require.NotEmpty(t, secret.Data[pgbackrest.TokenKey])
	})

	t.Run("failed refresh of the lent token keeps it", func(t *testing.T) {
		t.Parallel()
		r, c, rec := setup(t, "aws", roleARN, newBranch(s3Backup(pgbackrest.KeyTypeWebID), s3Restore), sourceSA())
		reconcile(t, r)

		secret := getSecret(t, c)
		secret.Annotations[PgBackRestTokenRefreshAnnotation] = time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
		require.NoError(t, c.Update(t.Context(), secret))
		rec.failTokenFor = "source-pgbackrest"
		reconcile(t, r)
		secret = getSecret(t, c)
		require.Equal(t, lentRequest, secret.Annotations[PgBackRestTokenRequestAnnotation])
		require.Equal(t, "token-1", string(secret.Data[pgbackrest.TokenKey]))
	})

	t.Run("Branch with a primary is never lent", func(t *testing.T) {
		t.Parallel()
		cluster := &apiv1.Cluster{
			Name: "cluster", Namespace: "xata-clusters",
			Status: apiv1.ClusterStatus{CurrentPrimary: "cluster-1"},
		}
		r, c, rec := setup(t, "aws", roleARN, newBranch(s3Backup(pgbackrest.KeyTypeWebID), s3Restore), sourceSA(), cluster)

		reconcile(t, r)
		require.Equal(t, v1alpha1.RestoreSourceAccessReleasedReason,
			getCondition(t, c, v1alpha1.RestoreSourceAccessConditionType).Reason)
		require.Equal(t, []string{"branch-pgbackrest"}, rec.tokensFor)
	})

	t.Run("without a restore role the Cluster is not applied", func(t *testing.T) {
		t.Parallel()
		r, c, rec := setup(t, "aws", "", newBranch(s3Backup(pgbackrest.KeyTypeWebID), s3Restore), sourceSA())

		res := reconcile(t, r)
		require.Zero(t, res.RequeueAfter)
		requireNotReady(t, c, v1alpha1.RestoreSourceRoleNotConfiguredReason)
		require.Empty(t, rec.tokensFor)
	})

	t.Run("waits for the source ServiceAccount", func(t *testing.T) {
		t.Parallel()
		r, c, rec := setup(t, "aws", roleARN, newBranch(s3Backup(pgbackrest.KeyTypeWebID), s3Restore))

		res := reconcile(t, r)
		require.Equal(t, restoreSourceRetryDelay, res.RequeueAfter)
		ready := requireNotReady(t, c, v1alpha1.AwaitingRestoreSourceAccessReason)
		require.Contains(t, ready.Message, "source-pgbackrest")
		require.Empty(t, rec.tokensFor)

		require.NoError(t, c.Create(t.Context(), sourceSA()))
		reconcile(t, r)
		require.NoError(t, c.Get(t.Context(), clusterKey, &apiv1.Cluster{}))
		require.Equal(t, []string{"source-pgbackrest"}, rec.tokensFor)
	})

	unchanged := map[string]struct {
		cloudProvider string
		branch        *v1alpha1.Branch
	}{
		"Branch without a restore": {
			cloudProvider: "aws",
			branch:        newBranch(s3Backup(pgbackrest.KeyTypeWebID), nil),
		},
		"restore from the repository of the Branch": {
			cloudProvider: "aws",
			branch: newBranch(s3Backup(pgbackrest.KeyTypeWebID), &v1alpha1.RestoreSpec{
				Type: v1alpha1.RestoreTypeObjectStore, Name: "branch-1",
			}),
		},
		"source with an endpoint": {
			cloudProvider: "aws",
			branch: newBranch(s3Backup(pgbackrest.KeyTypeWebID), s3RestoreFrom(v1alpha1.PgBackRestS3Spec{
				Bucket: "some-bucket", Region: "us-east-1", Endpoint: "https://example.r2.cloudflarestorage.com",
				KeyType: pgbackrest.KeyTypeWebID,
			})),
		},
		"source in another bucket": {
			cloudProvider: "aws",
			branch: newBranch(s3Backup(pgbackrest.KeyTypeWebID), s3RestoreFrom(v1alpha1.PgBackRestS3Spec{
				Bucket: "other-bucket", Region: "us-east-1", KeyType: pgbackrest.KeyTypeWebID,
			})),
		},
		"source without web identity": {
			cloudProvider: "aws",
			branch: newBranch(s3Backup(pgbackrest.KeyTypeWebID), s3RestoreFrom(v1alpha1.PgBackRestS3Spec{
				Bucket: "some-bucket", Region: "us-east-1", KeyType: "auto",
			})),
		},
		"Branch without web identity": {
			cloudProvider: "aws",
			branch:        newBranch(s3Backup("auto"), s3Restore),
		},
	}
	for name, tc := range unchanged {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			r, c, rec := setup(t, tc.cloudProvider, roleARN, tc.branch, sourceSA())

			reconcile(t, r)
			cluster := &apiv1.Cluster{}
			require.NoError(t, c.Get(t.Context(), clusterKey, cluster))
			require.Nil(t, getCondition(t, c, v1alpha1.RestoreSourceAccessConditionType))
			require.NotContains(t, rec.tokensFor, "source-pgbackrest")
			for _, ec := range cluster.Spec.ExternalClusters {
				if ec.PgBackRest != nil && ec.PgBackRest.Repository.S3 != nil {
					require.Empty(t, ec.PgBackRest.Repository.S3.RoleARN)
				}
			}
		})
	}
}
