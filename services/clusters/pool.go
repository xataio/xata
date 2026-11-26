package clusters

import (
	"context"
	"fmt"
	"strings"
	"time"

	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"xata/internal/postgresversions"
	cpv1alpha1 "xata/proto/clusterpool-operator/api/v1alpha1"
)

// findPoolCluster finds a healthy, pre-provisioned cluster from a ClusterPool
// that matches the requested configuration (storage class, Postgres version,
// CPU, memory, and a disk no larger than the requested storage size).
func findPoolCluster(ctx context.Context, kubeClient client.Client, clusterReader client.Reader, namespace, storageClass, image, cpuRequest, memory, storageSize string) (string, *apiv1.Cluster, error) {
	var pools cpv1alpha1.ClusterPoolList
	if err := kubeClient.List(ctx, &pools, client.InNamespace(namespace)); err != nil {
		// If the ClusterPool CRD is not installed, treat it as if no matching pool
		// exists
		if meta.IsNoMatchError(err) {
			return "", nil, nil
		}
		return "", nil, fmt.Errorf("list cluster pools: %w", err)
	}

	requestedCPU := resource.MustParse(cpuRequest)
	requestedMemory := resource.MustParse(memory)
	requestedStorage := resource.MustParse(storageSize)

	for i := range pools.Items {
		pool := &pools.Items[i]
		spec := &pool.Spec.ClusterSpec

		poolSC := ""
		if spec.StorageConfiguration.StorageClass != nil {
			poolSC = *spec.StorageConfiguration.StorageClass
		}
		if poolSC != storageClass {
			continue
		}

		if postgresversions.ShortImageName(spec.ImageName) != postgresversions.ShortImageName(image) {
			continue
		}

		poolCPU := spec.Resources.Requests[corev1.ResourceCPU]
		poolMemory := spec.Resources.Requests[corev1.ResourceMemory]
		if !poolCPU.Equal(requestedCPU) || !poolMemory.Equal(requestedMemory) {
			continue
		}

		// CNPG cannot shrink volumes, so skip a larger pool disk before polling its clusters
		larger, err := storageLargerThan(spec.StorageConfiguration.Size, requestedStorage)
		if err != nil {
			return "", nil, fmt.Errorf("parse storage size of pool %s: %w", pool.Name, err)
		}
		if larger {
			continue
		}

		cluster, err := findAvailableClusterInPool(ctx, kubeClient, clusterReader, namespace, pool, requestedStorage)
		if err != nil {
			return "", nil, fmt.Errorf("find available cluster in pool %s: %w", pool.Name, err)
		}
		if cluster != nil {
			return pool.Name, cluster, nil
		}
	}

	return "", nil, nil
}

// storageLargerThan reports whether size is larger than the requested storage.
// An unset size is never larger.
func storageLargerThan(size string, requested resource.Quantity) (bool, error) {
	if size == "" {
		return false, nil
	}
	quantity, err := resource.ParseQuantity(size)
	if err != nil {
		return false, err
	}
	return quantity.Cmp(requested) > 0, nil
}

// slotPoolName derives the slot pool name from a pool name. Each pool (used
// for new main branches) has a companion slot pool (used when waking
// hibernated branches and for child branches). A name convention is used to
// match the pool with its slot pool. For example `pg18-3-micro` is matched
// with `pg18-3-micro-slot`.
func slotPoolName(poolName string) string {
	return strings.TrimSuffix(poolName, "-slot") + "-slot"
}

var (
	poolClusterWaitTimeout  = 15 * time.Second
	poolClusterPollInterval = 1 * time.Second
)

// findAvailableClusterInPool polls for a healthy cluster in the given pool,
// waiting up to poolClusterWaitTimeout. A short wait is used because the pool
// operator continuously replenishes clusters, so one may become ready shortly.
func findAvailableClusterInPool(
	ctx context.Context,
	kubeClient client.Client,
	clusterReader client.Reader,
	namespace string,
	pool *cpv1alpha1.ClusterPool,
	requestedStorage resource.Quantity,
) (*apiv1.Cluster, error) {
	deadline := time.Now().Add(poolClusterWaitTimeout)
	for {
		cluster, err := findHealthyClusterInPool(ctx, kubeClient, clusterReader, namespace, pool, requestedStorage)
		if err != nil {
			return nil, err
		}
		if cluster != nil {
			return cluster, nil
		}
		if time.Now().After(deadline) {
			return nil, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(poolClusterPollInterval):
		}
	}
}

// findHealthyClusterInPool returns the first cluster owned by the pool that is
// in the Healthy phase, not being deleted, and has a disk no larger than the
// requested storage.
func findHealthyClusterInPool(
	ctx context.Context,
	kubeClient client.Client,
	clusterReader client.Reader,
	namespace string,
	pool *cpv1alpha1.ClusterPool,
	requestedStorage resource.Quantity,
) (*apiv1.Cluster, error) {
	var clusters apiv1.ClusterList
	if err := clusterReader.List(ctx, &clusters,
		client.InNamespace(namespace),
		client.MatchingFields{clusterOwnerKey: pool.Name},
	); err != nil {
		return nil, fmt.Errorf("list clusters: %w", err)
	}

	for i := range clusters.Items {
		cluster := &clusters.Items[i]
		if cluster.DeletionTimestamp != nil {
			continue
		}
		// CNPG rejects shrinking existing clusters when a pool's size is lowered
		larger, err := storageLargerThan(cluster.Spec.StorageConfiguration.Size, requestedStorage)
		if err != nil {
			return nil, fmt.Errorf("parse storage size of cluster %s: %w", cluster.Name, err)
		}
		if larger {
			continue
		}
		if cluster.Status.ReadyInstances > 0 {
			if err := orphanCluster(ctx, kubeClient, cluster); err != nil {
				if apierrors.IsConflict(err) {
					// A conflict indicates that another caller claimed the cluster
					// concurrently
					continue
				}
				return nil, fmt.Errorf("orphan cluster %s: %w", cluster.Name, err)
			}
			return cluster, nil
		}
	}

	return nil, nil
}

// orphanCluster removes the cluster from its pool by clearing its
// ownerReferences. This prevents the pool operator from managing or deleting
// the cluster once it has been assigned to a branch. The patch uses
// optimistic concurrency control: if the cluster's resourceVersion has
// changed since it was read (e.g. another caller claimed it concurrently),
// the API server returns a Conflict error.
func orphanCluster(ctx context.Context, kubeClient client.Client, cluster *apiv1.Cluster) error {
	patch := client.MergeFromWithOptions(cluster.DeepCopy(), client.MergeFromWithOptimisticLock{})
	cluster.OwnerReferences = nil

	if err := kubeClient.Patch(ctx, cluster, patch); err != nil {
		return fmt.Errorf("patch cluster %s: %w", cluster.Name, err)
	}

	return nil
}
