package wakeup

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/shared"
	"xata/services/branch-operator/pkg/wakeup/tracing"
)

func TestWaitForRolePasswordSyncUsesBranchLogin(t *testing.T) {
	tests := map[string]struct {
		statusRole string
		login      string
		want       string
	}{
		"white label login synced":          {statusRole: "dbadmin", login: shared.WhiteLabelSQLIdentity.Login, want: tracing.PasswordSyncOutcomeSynced},
		"legacy login synced":               {statusRole: "xata", login: shared.LegacySQLIdentity.Login, want: tracing.PasswordSyncOutcomeSynced},
		"white label ignores legacy status": {statusRole: "xata", login: shared.WhiteLabelSQLIdentity.Login, want: tracing.PasswordSyncOutcomeTimedOut},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, clientgoscheme.AddToScheme(scheme))
			require.NoError(t, apiv1.AddToScheme(scheme))
			require.NoError(t, v1alpha1.AddToScheme(scheme))

			secret := &corev1.Secret{
				Name: "b1-app", Namespace: "ns", ResourceVersion: "7",
			}
			cluster := &apiv1.Cluster{
				Name: "c1", Namespace: "ns",
			}
			c := fake.NewClientBuilder().WithScheme(scheme).
				WithObjects(secret, cluster, &v1alpha1.Branch{Name: "b1"}).
				Build()

			// Stamp the status with the version the fake client gave the Secret
			got := &corev1.Secret{}
			require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(secret), got))
			cluster.Status.ManagedRolesStatus.PasswordStatus = map[string]apiv1.PasswordState{
				tc.statusRole: {SecretResourceVersion: got.ResourceVersion},
			}
			require.NoError(t, c.Update(context.Background(), cluster))

			r := &WakeupReconciler{Client: c, PasswordSyncTimeout: 0}
			outcome, err := r.waitForRolePasswordSync(context.Background(), logr.Discard(), "b1", tc.login, cluster)
			require.NoError(t, err)
			require.Equal(t, tc.want, outcome)
		})
	}
}
