package reconciler_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"xata/services/branch-operator/api/v1alpha1"
)

func TestAdditionalServicesReconciliation(t *testing.T) {
	t.Parallel()

	t.Run("additional services are created on branch creation", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithInstances(2).
			WithPooler().
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			suffixes := []string{"-rw", "-r", "-ro", "-pooler"}

			for _, suffix := range suffixes {
				svcName := "branch-" + br.Name + suffix

				requireEventuallyNoErr(t, func() error {
					return getK8SObject(ctx, svcName, &v1.Service{})
				})
			}
		})
	})

	t.Run("additional services are owned by the Branch", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithInstances(2).
			WithPooler().
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			suffixes := []string{"-rw", "-r", "-ro", "-pooler"}

			for _, suffix := range suffixes {
				svc := v1.Service{}
				svcName := "branch-" + br.Name + suffix

				requireEventuallyNoErr(t, func() error {
					return getK8SObject(ctx, svcName, &svc)
				})
				require.Len(t, svc.GetOwnerReferences(), 1)
				require.Equal(t, br.Name, svc.GetOwnerReferences()[0].Name)
			}
		})
	})

	t.Run("single instance branch does not have a read-only service", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		// Create a Branch with a single instance
		branch := NewBranchBuilder().WithInstances(1).Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			// Wait for reconciliation to complete by checking the Branch is ready
			requireEventuallyTrue(t, func() bool {
				b := v1alpha1.Branch{}
				if err := k8sClient.Get(ctx, client.ObjectKey{Name: br.Name}, &b); err != nil {
					return false
				}
				return meta.IsStatusConditionTrue(b.Status.Conditions, v1alpha1.BranchReadyConditionType)
			})

			// Verify no read-only Service was created
			svc := v1.Service{}
			err := getK8SObject(ctx, "branch-"+br.Name+"-ro", &svc)
			require.True(t, apierrors.IsNotFound(err))
		})
	})

	t.Run("read-only service is deleted when branch is scaled down to a single instance", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		// Create a Branch with a replica
		branch := NewBranchBuilder().WithInstances(2).Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			svcName := "branch-" + br.Name + "-ro"

			// Expect the read-only Service to be created
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, svcName, &v1.Service{})
			})

			// Scale the Branch down to a single instance
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Instances = 1
			})
			require.NoError(t, err)

			// Expect the read-only Service to be deleted
			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, svcName, &v1.Service{})
				return apierrors.IsNotFound(err)
			})
		})
	})

	t.Run("pooler service is not created when pooler is disabled", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		// Create a Branch without a pooler
		branch := NewBranchBuilder().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			// Wait for reconciliation to complete by checking the Branch is ready
			requireEventuallyTrue(t, func() bool {
				b := v1alpha1.Branch{}
				if err := k8sClient.Get(ctx, client.ObjectKey{Name: br.Name}, &b); err != nil {
					return false
				}
				return b.Status.ObservedGeneration == b.Generation
			})

			// Verify no pooler Service was created
			svc := v1.Service{}
			err := getK8SObject(ctx, "branch-"+br.Name+"-pooler", &svc)
			require.True(t, apierrors.IsNotFound(err))
		})
	})
}
