package reconciler

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"xata/internal/pgbackrest"
	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/gcp/gcsfolder"
	"xata/services/branch-operator/pkg/reconciler/resources"
)

//nolint:gosec // G101: annotation names, not credentials.
const (
	// PgBackRestTokenRequestAnnotation records the request that issued the
	// token in the Secret. A different request, for example after a
	// ServiceAccount is created again or the lifetime changes, gets a new
	// token at once.
	PgBackRestTokenRequestAnnotation = "xata.io/token-request"
	// PgBackRestTokenExpirationAnnotation records when the token in the Secret
	// expires.
	PgBackRestTokenExpirationAnnotation = "xata.io/token-expiration"
	// PgBackRestTokenRefreshAnnotation records when the operator replaces the
	// token in the Secret.
	PgBackRestTokenRefreshAnnotation = "xata.io/token-refresh-after"

	// pgBackRestAWSTokenAudience is the audience AWS STS requires in a web
	// identity token.
	pgBackRestAWSTokenAudience = "sts.amazonaws.com"
	// pgBackRestTokenLifetime is the requested token lifetime, the same as the
	// EKS IRSA webhook default. AWS STS does not check whether the
	// ServiceAccount still exists, so the lifetime is also the time a token
	// stays valid after the Branch no longer uses it. The API server can
	// issue a shorter token; the operator uses the returned expiration.
	pgBackRestTokenLifetime = 24 * time.Hour
	// pgBackRestTokenRefreshFraction is the fraction of the token lifetime
	// after which the operator requests a new token. The kubelet uses the
	// same value for projected ServiceAccount tokens.
	pgBackRestTokenRefreshFraction = 0.8
	// pgBackRestTokenRefreshJitter is the maximum random time by which a
	// refresh comes earlier, so Branches created together do not refresh
	// together.
	pgBackRestTokenRefreshJitter = 30 * time.Minute
	// pgBackRestTokenRetryDelay is the minimum time before the operator tries
	// again after a failed refresh while the current token is still valid. A
	// random time up to the same value is added, so failed refreshes do not
	// retry together.
	pgBackRestTokenRetryDelay = 5 * time.Minute

	// pgBackRestRestoreTokenAudience is the audience of a token that a Branch
	// borrows to restore from the backups of another Branch. Only the
	// read-only restore role accepts it.
	pgBackRestRestoreTokenAudience = "sts.amazonaws.com/restore"
	// pgBackRestRestoreTokenLifetime is the lifetime of a borrowed token. AWS
	// STS accepts the token until it expires, also after the restore.
	pgBackRestRestoreTokenLifetime = time.Hour
	// restoreSourceRetryDelay is the time between checks for the
	// ServiceAccount of a restore source. The operator does not watch it.
	restoreSourceRetryDelay = time.Minute
)

// reconcilePgBackRestIdentity keeps a ServiceAccount for the Branch and a
// token for it in a Secret named after the cluster of the Branch. It refreshes
// the token before it expires and returns the time until the next refresh.
//
// The Branch owns the ServiceAccount, so the ServiceAccount follows the Branch
// lifecycle. The Cluster owns the Secret, so Kubernetes deletes the Secret
// with the Cluster: when the Branch hibernates, moves to another cluster, or
// is deleted. A token for the Branch never stays in the Secret of a cluster
// the Branch no longer uses. A restore can write the Secret before the
// Cluster exists. Until then the Branch owns it.
//
// On AWS, the operator keeps the identity for each Branch that can use it,
// also if the Branch does not use it. Only Branches with s3.keyType=web-id use
// the token. Thus, a Branch that changes to web-id has a token immediately.
func (r *BranchReconciler) reconcilePgBackRestIdentity(
	ctx context.Context,
	branch *v1alpha1.Branch,
) (time.Duration, error) {
	if !r.usesPgBackRestBranchIdentity(branch) {
		return 0, nil
	}

	sa, err := r.reconcilePgBackRestServiceAccount(ctx, branch)
	if err != nil {
		return 0, fmt.Errorf("reconcile pgbackrest ServiceAccount: %w", err)
	}

	if !branch.HasClusterName() {
		return 0, nil
	}

	// The Cluster was applied earlier in this reconcile, but a new Cluster can
	// be missing from the cache for a short time. The error makes
	// controller-runtime try again with backoff.
	cluster := &apiv1.Cluster{}
	key := client.ObjectKey{Name: branch.ClusterName(), Namespace: r.ClustersNamespace}
	if err := r.Get(ctx, key, cluster); err != nil {
		return 0, fmt.Errorf("get Cluster %s: %w", key.Name, err)
	}

	audience, lifetime := r.pgBackRestTokenAudience(), pgBackRestTokenLifetime
	if r.lendsRestoreSourceToken(branch) {
		// The restore reads the repository of the source with its token.
		if sa, err = r.getRestoreSourceServiceAccount(ctx, branch); err != nil {
			return 0, fmt.Errorf("get restore source ServiceAccount: %w", err)
		}
		audience, lifetime = pgBackRestRestoreTokenAudience, pgBackRestRestoreTokenLifetime
	}

	refreshIn, err := r.reconcilePgBackRestToken(ctx, branch, cluster, sa, audience, lifetime)
	if err != nil {
		return 0, fmt.Errorf("reconcile pgbackrest web identity Secret: %w", err)
	}
	return refreshIn, nil
}

// usesPgBackRestBranchIdentity reports whether the Branch needs a pgBackRest
// ServiceAccount and token.
func (r *BranchReconciler) usesPgBackRestBranchIdentity(branch *v1alpha1.Branch) bool {
	switch r.CloudProvider {
	case cloudProviderAWS:
		return r.BackupsEndpoint == "" && resources.UsesAWSIAM(branch.Spec.BackupSpec)
	case cloudProviderGCP:
		return r.PgBackRestWorkloadIdentityPool != "" && usesGCS(branch.Spec.BackupSpec)
	default:
		return false
	}
}

func usesGCS(b *v1alpha1.BackupSpec) bool {
	return b.IsPgBackRest() && b.PgBackRest.GCS != nil
}

// pgBackRestTokenAudience returns the token audience that the STS of the cloud
// provider accepts.
func (r *BranchReconciler) pgBackRestTokenAudience() string {
	if r.CloudProvider == cloudProviderGCP {
		return r.PgBackRestWorkloadIdentityPool
	}
	return pgBackRestAWSTokenAudience
}

func (r *BranchReconciler) reconcilePgBackRestServiceAccount(
	ctx context.Context,
	branch *v1alpha1.Branch,
) (*corev1.ServiceAccount, error) {
	sa := &corev1.ServiceAccount{
		Name:      pgbackrest.ServiceAccountName(branch.Name),
		Namespace: r.ClustersNamespace,
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sa, func() error {
		if err := controllerutil.SetControllerReference(branch, sa, r.Scheme); err != nil {
			return err
		}
		ensureLabels(sa, branch.Spec.InheritedMetadata)
		if b := branch.Spec.BackupSpec; usesGCS(b) && b.PgBackRest.GCS.KeyType == pgbackrest.KeyTypeWebID {
			if sa.Annotations == nil {
				sa.Annotations = map[string]string{}
			}
			sa.Annotations[pgbackrest.GCSFolderAnnotation] = pgbackrest.GCSFolderURI(
				b.PgBackRest.GCS.Bucket, b.PgBackRest.GetRepoPath(branch.Name),
			)
			if folder := r.restoreSourceFolder(branch); folder != "" && !restoreSourceAccessReleased(branch) {
				sa.Annotations[pgbackrest.GCSRestoreFolderAnnotation] = folder
			} else if sa.Annotations[pgbackrest.GCSRestoreFolderAnnotation] == sa.Annotations[gcsfolder.ObservedRestoreFolderAnnotation] {
				// Remove the request only once observed, or its grant leaks.
				delete(sa.Annotations, pgbackrest.GCSRestoreFolderAnnotation)
			}
		}
		// The token is only for pgBackRest, which reads it from the Secret.
		sa.AutomountServiceAccountToken = new(false)
		return nil
	})

	return sa, err
}

// reconcileRestoreSourceAccess gives the Branch read access to the repository
// of its restore source before the Cluster bootstraps from it, and releases
// it once the Cluster has a primary. While the access is not ready, it returns
// false and the time after which to check again, or zero to wait for a watch.
func (r *BranchReconciler) reconcileRestoreSourceAccess(
	ctx context.Context,
	branch *v1alpha1.Branch,
) (bool, time.Duration, error) {
	folder := r.restoreSourceFolder(branch)
	borrow := r.restoreBorrowsSourceToken(branch)
	if (folder == "" && !borrow) || !branch.HasClusterName() || restoreSourceAccessReleased(branch) {
		return true, 0, nil
	}

	cluster := &apiv1.Cluster{}
	key := client.ObjectKey{Name: branch.ClusterName(), Namespace: r.ClustersNamespace}
	if err := r.Get(ctx, key, cluster); apierrors.IsNotFound(err) {
		cluster = nil
	} else if err != nil {
		return false, 0, fmt.Errorf("get Cluster %s: %w", key.Name, err)
	} else if cluster.Status.CurrentPrimary != "" {
		setStatusCondition(branch, v1alpha1.RestoreSourceAccessConditionType,
			metav1.ConditionFalse, v1alpha1.RestoreSourceAccessReleasedReason)
		// Persist the release before acting on it, or a lost write lends again.
		if err := r.applyStatus(ctx, branch); err != nil {
			return false, 0, fmt.Errorf("persist restore source release: %w", err)
		}
		return true, 0, nil
	}

	var wait restoreSourceWait
	var err error
	if folder != "" {
		wait, err = r.requestRestoreSourceFolder(ctx, branch, folder)
	} else {
		wait, err = r.lendRestoreSourceToken(ctx, branch, cluster)
	}
	if err != nil {
		return false, 0, err
	}
	if wait.reason != "" {
		meta.SetStatusCondition(&branch.Status.Conditions, metav1.Condition{
			Type:               v1alpha1.BranchReadyConditionType,
			Status:             metav1.ConditionFalse,
			Reason:             wait.reason,
			Message:            strings.TrimSpace(v1alpha1.BranchConditionMessages[wait.reason] + " " + wait.detail),
			ObservedGeneration: branch.Generation,
		})
		return false, wait.retryAfter, nil
	}
	setStatusCondition(branch, v1alpha1.RestoreSourceAccessConditionType,
		metav1.ConditionTrue, v1alpha1.RestoreSourceAccessGrantedReason)
	return true, 0, nil
}

// restoreSourceWait says why the restore cannot start yet. An empty reason
// means that it can.
type restoreSourceWait struct {
	reason     string
	detail     string
	retryAfter time.Duration
}

// requestRestoreSourceFolder asks gcsfolder for read access to the GCS folder
// of the restore source. The ServiceAccount watch wakes the reconcile when the
// grant is observed.
func (r *BranchReconciler) requestRestoreSourceFolder(
	ctx context.Context,
	branch *v1alpha1.Branch,
	folder string,
) (restoreSourceWait, error) {
	sa, err := r.reconcilePgBackRestServiceAccount(ctx, branch)
	if err != nil {
		return restoreSourceWait{}, fmt.Errorf("reconcile pgbackrest ServiceAccount: %w", err)
	}
	if sa.Annotations[gcsfolder.ObservedRestoreFolderAnnotation] != folder {
		return restoreSourceWait{reason: v1alpha1.AwaitingRestoreSourceAccessReason, detail: folder}, nil
	}
	return restoreSourceWait{}, nil
}

// lendRestoreSourceToken writes a token of the ServiceAccount of the restore
// source to the Secret before the Cluster exists, so the bootstrap job mounts
// it at start. Once the Cluster exists, reconcilePgBackRestIdentity keeps it.
func (r *BranchReconciler) lendRestoreSourceToken(
	ctx context.Context,
	branch *v1alpha1.Branch,
	cluster *apiv1.Cluster,
) (restoreSourceWait, error) {
	if r.PgBackRestS3RestoreRoleARN == "" {
		return restoreSourceWait{reason: v1alpha1.RestoreSourceRoleNotConfiguredReason}, nil
	}
	sa, err := r.getRestoreSourceServiceAccount(ctx, branch)
	if apierrors.IsNotFound(err) {
		return restoreSourceWait{
			reason:     v1alpha1.AwaitingRestoreSourceAccessReason,
			detail:     "ServiceAccount " + pgbackrest.ServiceAccountName(branch.Spec.Restore.Name),
			retryAfter: restoreSourceRetryDelay,
		}, nil
	}
	if err != nil {
		return restoreSourceWait{}, fmt.Errorf("get restore source ServiceAccount: %w", err)
	}
	if cluster == nil {
		_, err := r.reconcilePgBackRestToken(ctx, branch, nil, sa,
			pgBackRestRestoreTokenAudience, pgBackRestRestoreTokenLifetime)
		if err != nil {
			return restoreSourceWait{}, fmt.Errorf("lend restore source token: %w", err)
		}
	}
	return restoreSourceWait{}, nil
}

// restoreSourceFolder returns the GCS folder of the restore source that the
// Branch ServiceAccount needs read access to, or "" if it needs none.
func (r *BranchReconciler) restoreSourceFolder(branch *v1alpha1.Branch) string {
	b := branch.Spec.BackupSpec
	// Without a project number the operator runs no gcsfolder reconciler.
	if r.CloudProvider != cloudProviderGCP || r.PgBackRestWorkloadIdentityPool == "" || r.PgBackRestGCSProjectNumber == "" ||
		!usesGCS(b) || b.PgBackRest.GCS.KeyType != pgbackrest.KeyTypeWebID {
		return ""
	}
	rs := branch.Spec.Restore
	// The source must be in the backup bucket of the Branch.
	if !rs.IsObjectStoreType() || rs.PgBackRest == nil || rs.PgBackRest.GCS == nil ||
		rs.PgBackRest.GCS.KeyType != pgbackrest.KeyTypeWebID || rs.PgBackRest.GCS.Bucket != b.PgBackRest.GCS.Bucket {
		return ""
	}
	return pgbackrest.GCSFolderURI(rs.PgBackRest.GCS.Bucket, rs.PgBackRest.RepoPath)
}

// restoreBorrowsSourceToken reports whether the restore reads an S3 source
// with a token of the source ServiceAccount. IAM lets a token read only the
// prefix of its own ServiceAccount.
func (r *BranchReconciler) restoreBorrowsSourceToken(branch *v1alpha1.Branch) bool {
	b := branch.Spec.BackupSpec
	if r.CloudProvider != cloudProviderAWS || !r.usesPgBackRestBranchIdentity(branch) ||
		b.PgBackRest.S3.KeyType != pgbackrest.KeyTypeWebID {
		return false
	}
	rs := branch.Spec.Restore
	// The source must be in the backup bucket of the Branch.
	return rs.IsObjectStoreType() && rs.PgBackRest != nil && rs.PgBackRest.S3 != nil &&
		rs.PgBackRest.S3.KeyType == pgbackrest.KeyTypeWebID && rs.PgBackRest.S3.Endpoint == "" &&
		rs.PgBackRest.S3.Bucket == b.PgBackRest.S3.Bucket
}

// lendsRestoreSourceToken reports whether the Secret holds a token of the
// restore source and not of the Branch.
func (r *BranchReconciler) lendsRestoreSourceToken(branch *v1alpha1.Branch) bool {
	return r.PgBackRestS3RestoreRoleARN != "" && r.restoreBorrowsSourceToken(branch) &&
		!restoreSourceAccessReleased(branch)
}

// restoreRoleARN returns the role for the S3 repository of the restore
// source, or "" if the restore does not borrow the token of the source.
func (r *BranchReconciler) restoreRoleARN(branch *v1alpha1.Branch) string {
	if !r.restoreBorrowsSourceToken(branch) {
		return ""
	}
	return r.PgBackRestS3RestoreRoleARN
}

func (r *BranchReconciler) getRestoreSourceServiceAccount(
	ctx context.Context,
	branch *v1alpha1.Branch,
) (*corev1.ServiceAccount, error) {
	sa := &corev1.ServiceAccount{}
	key := client.ObjectKey{
		Name:      pgbackrest.ServiceAccountName(branch.Spec.Restore.Name),
		Namespace: r.ClustersNamespace,
	}
	return sa, r.Get(ctx, key, sa)
}

func restoreSourceAccessReleased(branch *v1alpha1.Branch) bool {
	c := meta.FindStatusCondition(branch.Status.Conditions, v1alpha1.RestoreSourceAccessConditionType)
	return c != nil && c.Reason == v1alpha1.RestoreSourceAccessReleasedReason
}

// reconcilePgBackRestToken writes a new token to the Secret when the Secret
// has no token, a token from a different request, or a token that is due for
// refresh. When a refresh fails and the current token is still valid, it keeps
// the token and tries again later, as the kubelet does. The cluster is nil
// before the Cluster exists.
func (r *BranchReconciler) reconcilePgBackRestToken(
	ctx context.Context,
	branch *v1alpha1.Branch,
	cluster *apiv1.Cluster,
	sa *corev1.ServiceAccount,
	audience string,
	lifetime time.Duration,
) (time.Duration, error) {
	secret := &corev1.Secret{
		Name:      pgbackrest.WebIdentitySecretName(branch.ClusterName()),
		Namespace: r.ClustersNamespace,
	}

	tr := &authenticationv1.TokenRequest{
		// The API server rejects the request when the ServiceAccount was
		// deleted and created again after the operator read it.
		UID: sa.UID,
		Spec: authenticationv1.TokenRequestSpec{
			Audiences:         []string{audience},
			ExpirationSeconds: new(int64(lifetime.Seconds())),
		},
	}
	request := pgBackRestTokenRequestKey(sa, tr)

	// Request the token before the write and not in the CreateOrUpdate mutate
	// function, so the mutate function has no side effects.
	if err := r.Get(ctx, client.ObjectKeyFromObject(secret), secret); client.IgnoreNotFound(err) != nil {
		return 0, fmt.Errorf("get Secret %s: %w", secret.Name, err)
	}
	now := time.Now()
	current := currentPgBackRestToken(secret, request)
	next := current.refreshAt
	issued := false // true when tr has a new token to write to the secret
	if !current.valid || !now.Before(current.refreshAt) {
		if err := r.SubResource("token").Create(ctx, sa, tr); err != nil {
			if !current.valid || !now.Before(current.expiresAt) {
				err = fmt.Errorf("request token for ServiceAccount %s: %w", sa.Name, err)
				if audience != pgBackRestRestoreTokenAudience && holdsLentPgBackRestToken(secret) {
					// Do not keep the lent token after the release.
					return 0, errors.Join(err, r.clearPgBackRestToken(ctx, secret))
				}
				return 0, err
			}
			ctrl.LoggerFrom(ctx).Error(err, "refreshing pgbackrest token, keeping the current token",
				"serviceAccount", sa.Name, "expiresAt", current.expiresAt)
			next = now.Add(pgBackRestTokenRetryDelay + randDuration(pgBackRestTokenRetryDelay))
			if next.After(current.expiresAt) {
				next = current.expiresAt
			}
		} else {
			issued = true
			next = pgBackRestRefreshTime(now, tr.Status.ExpirationTimestamp.Time)
		}
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		if err := r.setPgBackRestTokenOwner(branch, cluster, secret); err != nil {
			return err
		}
		ensureLabels(secret, branch.Spec.InheritedMetadata)
		if issued {
			if secret.Annotations == nil {
				secret.Annotations = map[string]string{}
			}
			secret.Annotations[PgBackRestTokenRequestAnnotation] = request
			secret.Annotations[PgBackRestTokenExpirationAnnotation] = tr.Status.ExpirationTimestamp.UTC().Format(time.RFC3339)
			secret.Annotations[PgBackRestTokenRefreshAnnotation] = next.UTC().Format(time.RFC3339)
			secret.Data = map[string][]byte{pgbackrest.TokenKey: []byte(tr.Status.Token)}
		}
		return r.setGCSCredentials(secret)
	})
	if err != nil {
		return 0, err
	}

	return max(time.Until(next), time.Second), nil
}

// holdsLentPgBackRestToken reports whether the Secret holds a token of a
// restore source.
func holdsLentPgBackRestToken(secret *corev1.Secret) bool {
	return strings.Contains(secret.Annotations[PgBackRestTokenRequestAnnotation],
		"audiences=["+pgBackRestRestoreTokenAudience+"]")
}

// clearPgBackRestToken removes the token and its annotations from the Secret.
func (r *BranchReconciler) clearPgBackRestToken(ctx context.Context, secret *corev1.Secret) error {
	patch := client.MergeFrom(secret.DeepCopy())
	delete(secret.Data, pgbackrest.TokenKey)
	delete(secret.Annotations, PgBackRestTokenRequestAnnotation)
	delete(secret.Annotations, PgBackRestTokenExpirationAnnotation)
	delete(secret.Annotations, PgBackRestTokenRefreshAnnotation)
	if err := r.Patch(ctx, secret, patch); err != nil {
		return fmt.Errorf("clear lent token from Secret %s: %w", secret.Name, err)
	}
	return nil
}

// setPgBackRestTokenOwner makes the Cluster the owner of the Secret, so the
// Secret is deleted with the Cluster. Until the Cluster exists, the Branch
// owns it.
func (r *BranchReconciler) setPgBackRestTokenOwner(branch *v1alpha1.Branch, cluster *apiv1.Cluster, secret *corev1.Secret) error {
	if cluster == nil {
		return controllerutil.SetOwnerReference(branch, secret, r.Scheme)
	}
	// A plain owner reference and not a controller reference, so the CNPG
	// operator does not reconcile the Cluster when the Secret changes.
	if err := controllerutil.SetOwnerReference(cluster, secret, r.Scheme); err != nil {
		return err
	}
	owned, err := controllerutil.HasOwnerReference(secret.OwnerReferences, branch, r.Scheme)
	if err != nil || !owned {
		return err
	}
	return controllerutil.RemoveOwnerReference(branch, secret, r.Scheme)
}

// setGCSCredentials writes the external account credential file for the token
// in the Secret.
func (r *BranchReconciler) setGCSCredentials(secret *corev1.Secret) error {
	if r.CloudProvider != cloudProviderGCP || r.PgBackRestWorkloadIdentityPool == "" {
		return nil
	}
	issuer, err := tokenIssuer(string(secret.Data[pgbackrest.TokenKey]))
	if err != nil {
		return fmt.Errorf("read token issuer: %w", err)
	}
	credentials, err := pgbackrest.GCSCredentials(
		r.PgBackRestWorkloadIdentityPool, issuer)
	if err != nil {
		return fmt.Errorf("build GCS credential file: %w", err)
	}
	secret.Data[pgbackrest.GCSCredentialsKey] = credentials
	return nil
}

// tokenIssuer returns the `iss` claim of a ServiceAccount token.
func tokenIssuer(token string) (string, error) {
	var claims jwt.RegisteredClaims
	if _, _, err := jwt.NewParser().ParseUnverified(token, &claims); err != nil {
		return "", fmt.Errorf("parse token: %w", err)
	}
	if claims.Issuer == "" {
		return "", errors.New("token has no issuer")
	}
	return claims.Issuer, nil
}

// pgBackRestTokenRequestKey identifies the token request, like the cache key
// of the kubelet token manager.
func pgBackRestTokenRequestKey(sa *corev1.ServiceAccount, tr *authenticationv1.TokenRequest) string {
	return fmt.Sprintf("serviceAccount=%s,uid=%s,audiences=%v,expirationSeconds=%d",
		sa.Name, sa.UID, tr.Spec.Audiences, *tr.Spec.ExpirationSeconds)
}

type pgBackRestToken struct {
	// valid is false when the Secret has no token from the same request.
	valid     bool
	expiresAt time.Time
	refreshAt time.Time
}

// currentPgBackRestToken reads the token state from the Secret annotations.
func currentPgBackRestToken(secret *corev1.Secret, request string) pgBackRestToken {
	if len(secret.Data[pgbackrest.TokenKey]) == 0 ||
		secret.Annotations[PgBackRestTokenRequestAnnotation] != request {
		return pgBackRestToken{}
	}
	expiresAt, err := time.Parse(time.RFC3339, secret.Annotations[PgBackRestTokenExpirationAnnotation])
	if err != nil {
		return pgBackRestToken{}
	}
	refreshAt, err := time.Parse(time.RFC3339, secret.Annotations[PgBackRestTokenRefreshAnnotation])
	if err != nil {
		return pgBackRestToken{}
	}
	return pgBackRestToken{valid: true, expiresAt: expiresAt, refreshAt: refreshAt}
}

// pgBackRestRefreshTime returns the time after the refresh fraction of the
// token lifetime, less a random jitter.
func pgBackRestRefreshTime(issuedAt, expiresAt time.Time) time.Time {
	lifetime := expiresAt.Sub(issuedAt)
	jitter := min(randDuration(pgBackRestTokenRefreshJitter), lifetime/10)
	return issuedAt.Add(time.Duration(float64(lifetime)*pgBackRestTokenRefreshFraction) - jitter)
}

// randDuration returns a random duration in [0, d).
func randDuration(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	//nolint:gosec // jitter does not need a secure random source
	return rand.N(d)
}
