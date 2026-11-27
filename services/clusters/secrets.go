package clusters

import (
	"context"

	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clustersv1 "xata/gen/proto/clusters/v1"
	"xata/internal/passwords"
)

// createAppSecret creates the kubernetes.io/basic-auth Secret holding the login
// role credentials for a new branch. The caller supplies the password: freshly
// generated for a cluster that will be bootstrapped from scratch, or copied
// from the pool cluster's CNPG secret when adopting a cluster from a pool. If
// the secret already exists (a retried CreatePostgresCluster call, or a
// leftover from a failed attempt), its data is preserved — a password is never
// overwritten — and nil is returned so the caller does not clean it up on
// failure. The branch-operator adopts the secret (Branch ownership) on the
// first reconcile.
func (c *ClustersService) createAppSecret(ctx context.Context, name string, req *clustersv1.CreatePostgresClusterRequest, username, password string) (*corev1.Secret, error) {
	secret := &corev1.Secret{
		Name:      name,
		Namespace: c.config.ClustersNamespace,
		Labels: map[string]string{
			// Trigger reloads of the CNPG Cluster when the Secret changes
			"cnpg.io/reload": "true",
			LabelOrgID:       req.GetOrganizationId(),
			LabelProjectID:   req.GetProjectId(),
			LabelBranchID:    req.GetId(),
		},
		Type: corev1.SecretTypeBasicAuth,
		Data: map[string][]byte{
			corev1.BasicAuthUsernameKey: []byte(username),
			corev1.BasicAuthPasswordKey: []byte(password),
		},
	}

	if err := c.kubeClient.Create(ctx, secret); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil, nil
		}
		return nil, k8sErrorToGRPCError(err)
	}
	return secret, nil
}

// appPasswordForNewBranch returns the xata role password for a cluster. If the
// provided cluster is nil it generates a new password.
func (c *ClustersService) appPasswordForNewBranch(ctx context.Context, cluster *apiv1.Cluster) (string, error) {
	if cluster == nil {
		return passwords.Generate()
	}

	return c.poolClusterAppPassword(ctx, cluster)
}

// poolClusterAppPassword returns the xata role password of a Cluster
func (c *ClustersService) poolClusterAppPassword(ctx context.Context, cluster *apiv1.Cluster) (string, error) {
	secretName := cluster.Name + apiv1.ApplicationUserSecretSuffix
	secret := &corev1.Secret{}

	// Get the secret for the cluster's application user password
	err := c.kubeClient.Get(ctx, client.ObjectKey{
		Name:      secretName,
		Namespace: c.config.ClustersNamespace,
	}, secret)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", status.Errorf(codes.FailedPrecondition, "pool cluster %s has no app secret %s", cluster.Name, secretName)
		}
		return "", k8sErrorToGRPCError(err)
	}

	// Take the password from the secret
	password := secret.Data[corev1.BasicAuthPasswordKey]
	if len(password) == 0 {
		return "", status.Errorf(codes.FailedPrecondition, "pool cluster app secret %s has no password", secretName)
	}

	return string(password), nil
}
