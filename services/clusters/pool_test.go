package clusters

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cpv1alpha1 "xata/proto/clusterpool-operator/api/v1alpha1"
	branchv1alpha1 "xata/services/branch-operator/api/v1alpha1"
)

func TestFindPoolCluster(t *testing.T) {
	origTimeout := poolClusterWaitTimeout
	origInterval := poolClusterPollInterval
	poolClusterWaitTimeout = 100 * time.Millisecond
	poolClusterPollInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		poolClusterWaitTimeout = origTimeout
		poolClusterPollInterval = origInterval
	})

	const namespace = "xata-clusters"
	poolUID := types.UID("pool-uid-123")

	matchingPool := &cpv1alpha1.ClusterPool{
		Name:      "test-pool",
		Namespace: namespace,
		UID:       poolUID,
		Spec: cpv1alpha1.ClusterPoolSpec{
			Clusters: 2,
			ClusterSpec: apiv1.ClusterSpec{
				ImageName: "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
				StorageConfiguration: apiv1.StorageConfiguration{
					StorageClass: new("default-storage-class"),
					Size:         "250Gi",
				},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("2"),
						corev1.ResourceMemory: resource.MustParse("4Gi"),
					},
				},
			},
		},
	}

	availableCluster := &apiv1.Cluster{
		Name:      "pool-cluster-1",
		Namespace: namespace,
		OwnerReferences: []metav1.OwnerReference{
			{
				APIVersion: cpv1alpha1.GroupVersion.String(),
				Kind:       cpv1alpha1.ClusterPoolKind,
				UID:        poolUID,
				Name:       "test-pool",
				Controller: new(true),
			},
		},
		Status: apiv1.ClusterStatus{
			Phase:          apiv1.PhaseHealthy,
			ReadyInstances: 1,
		},
	}

	poolWithSize := func(size string) *cpv1alpha1.ClusterPool {
		pool := matchingPool.DeepCopy()
		pool.Spec.ClusterSpec.StorageConfiguration.Size = size
		return pool
	}

	// The image of a white label pool can have the short name of a legacy
	// image, so only the declared SQL identity tells the pools apart.
	whiteLabelPool := matchingPool.DeepCopy()
	whiteLabelPool.Spec.SQLIdentity = string(branchv1alpha1.SQLIdentityWhiteLabel)
	whiteLabelPool.Spec.ClusterSpec.ImageName = "registry.example.com/postgres:17.5@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	whiteLabelPool.Spec.ClusterSpec.Bootstrap = &apiv1.BootstrapConfiguration{
		InitDB: &apiv1.BootstrapInitDB{Database: "defaultdb", Owner: "dbadmin"},
	}
	whiteLabelPoolWithBootstrap := func(bootstrap *apiv1.BootstrapConfiguration) *cpv1alpha1.ClusterPool {
		pool := whiteLabelPool.DeepCopy()
		pool.Spec.ClusterSpec.Bootstrap = bootstrap
		return pool
	}

	tests := map[string]struct {
		objects      []client.Object
		sqlIdentity  branchv1alpha1.SQLIdentityProfile
		storageClass string
		image        string
		cpuRequest   string
		memory       string
		storageSize  string
		wantPoolName string
		wantName     string
		wantErr      bool
	}{
		"matching pool with available cluster": {
			objects:      []client.Object{matchingPool, availableCluster},
			storageClass: "default-storage-class",
			image:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
			cpuRequest:   "2",
			memory:       "4Gi",
			storageSize:  "250Gi",
			wantPoolName: "test-pool",
			wantName:     "pool-cluster-1",
		},
		"matching pool with a disk smaller than requested": {
			objects:      []client.Object{matchingPool, availableCluster},
			storageClass: "default-storage-class",
			image:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
			cpuRequest:   "2",
			memory:       "4Gi",
			storageSize:  "500Gi",
			wantPoolName: "test-pool",
			wantName:     "pool-cluster-1",
		},
		"no matching pool - disk larger than requested": {
			objects:      []client.Object{matchingPool, availableCluster},
			storageClass: "default-storage-class",
			image:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
			cpuRequest:   "2",
			memory:       "4Gi",
			storageSize:  "20Gi",
		},
		"matching pool without a disk size": {
			objects:      []client.Object{poolWithSize(""), availableCluster},
			storageClass: "default-storage-class",
			image:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
			cpuRequest:   "2",
			memory:       "4Gi",
			storageSize:  "20Gi",
			wantPoolName: "test-pool",
			wantName:     "pool-cluster-1",
		},
		"matching pool with an invalid disk size": {
			objects:      []client.Object{poolWithSize("not-a-size"), availableCluster},
			storageClass: "default-storage-class",
			image:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
			cpuRequest:   "2",
			memory:       "4Gi",
			storageSize:  "250Gi",
			wantErr:      true,
		},
		"no matching cluster - pool disk lowered below the cluster disk": {
			objects: []client.Object{poolWithSize("10Gi"), func() *apiv1.Cluster {
				cluster := availableCluster.DeepCopy()
				cluster.Spec.StorageConfiguration.Size = "250Gi"
				return cluster
			}()},
			storageClass: "default-storage-class",
			image:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
			cpuRequest:   "2",
			memory:       "4Gi",
			storageSize:  "20Gi",
		},
		"no matching pool - different storage class": {
			objects:      []client.Object{matchingPool, availableCluster},
			storageClass: "other-storage-class",
			image:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
			cpuRequest:   "2",
			memory:       "4Gi",
			storageSize:  "250Gi",
		},
		"no matching pool - different postgres major": {
			objects:      []client.Object{matchingPool, availableCluster},
			storageClass: "default-storage-class",
			image:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:16.3",
			cpuRequest:   "2",
			memory:       "4Gi",
			storageSize:  "250Gi",
		},
		"no matching pool - different postgres minor": {
			objects:      []client.Object{matchingPool, availableCluster},
			storageClass: "default-storage-class",
			image:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.2",
			cpuRequest:   "2",
			memory:       "4Gi",
			storageSize:  "250Gi",
		},
		"no matching pool - different image offering": {
			objects:      []client.Object{matchingPool, availableCluster},
			storageClass: "default-storage-class",
			image:        "ghcr.io/xataio/postgres-images/cnpg-postgres-analytics:17.5",
			cpuRequest:   "2",
			memory:       "4Gi",
			storageSize:  "250Gi",
		},
		"no matching pool - different cpu": {
			objects:      []client.Object{matchingPool, availableCluster},
			storageClass: "default-storage-class",
			image:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
			cpuRequest:   "4",
			memory:       "4Gi",
			storageSize:  "250Gi",
		},
		"no matching pool - different memory": {
			objects:      []client.Object{matchingPool, availableCluster},
			storageClass: "default-storage-class",
			image:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
			cpuRequest:   "2",
			memory:       "8Gi",
			storageSize:  "250Gi",
		},
		"matching pool but no available clusters": {
			objects:      []client.Object{matchingPool},
			storageClass: "default-storage-class",
			image:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
			cpuRequest:   "2",
			memory:       "4Gi",
			storageSize:  "250Gi",
		},
		"matching pool with unhealthy cluster": {
			objects: []client.Object{matchingPool, &apiv1.Cluster{
				Name:      "pool-cluster-unhealthy",
				Namespace: namespace,
				OwnerReferences: []metav1.OwnerReference{
					{APIVersion: cpv1alpha1.GroupVersion.String(), Kind: cpv1alpha1.ClusterPoolKind, UID: poolUID, Name: "test-pool", Controller: new(true)},
				},
				Status: apiv1.ClusterStatus{
					Phase: apiv1.PhaseWaitingForInstancesToBeActive,
				},
			}},
			storageClass: "default-storage-class",
			image:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
			cpuRequest:   "2",
			memory:       "4Gi",
			storageSize:  "250Gi",
		},
		"white label branch takes a cluster from a white label pool": {
			objects:      []client.Object{whiteLabelPool, availableCluster},
			sqlIdentity:  branchv1alpha1.SQLIdentityWhiteLabel,
			storageClass: "default-storage-class",
			image:        whiteLabelPool.Spec.ClusterSpec.ImageName,
			cpuRequest:   "2",
			memory:       "4Gi",
			storageSize:  "250Gi",
			wantPoolName: "test-pool",
			wantName:     "pool-cluster-1",
		},
		"white label pool with the legacy initdb names is skipped": {
			objects:      []client.Object{whiteLabelPoolWithBootstrap(&apiv1.BootstrapConfiguration{InitDB: &apiv1.BootstrapInitDB{Database: "xata", Owner: "xata"}}), availableCluster},
			sqlIdentity:  branchv1alpha1.SQLIdentityWhiteLabel,
			storageClass: "default-storage-class",
			image:        whiteLabelPool.Spec.ClusterSpec.ImageName,
			cpuRequest:   "2",
			memory:       "4Gi",
			storageSize:  "250Gi",
		},
		"white label pool with a legacy initdb owner is skipped": {
			objects:      []client.Object{whiteLabelPoolWithBootstrap(&apiv1.BootstrapConfiguration{InitDB: &apiv1.BootstrapInitDB{Database: "defaultdb", Owner: "xata"}}), availableCluster},
			sqlIdentity:  branchv1alpha1.SQLIdentityWhiteLabel,
			storageClass: "default-storage-class",
			image:        whiteLabelPool.Spec.ClusterSpec.ImageName,
			cpuRequest:   "2",
			memory:       "4Gi",
			storageSize:  "250Gi",
		},
		"white label pool with a legacy initdb database is skipped": {
			objects:      []client.Object{whiteLabelPoolWithBootstrap(&apiv1.BootstrapConfiguration{InitDB: &apiv1.BootstrapInitDB{Database: "xata", Owner: "dbadmin"}}), availableCluster},
			sqlIdentity:  branchv1alpha1.SQLIdentityWhiteLabel,
			storageClass: "default-storage-class",
			image:        whiteLabelPool.Spec.ClusterSpec.ImageName,
			cpuRequest:   "2",
			memory:       "4Gi",
			storageSize:  "250Gi",
		},
		"white label pool with a noop bootstrap is skipped": {
			objects:      []client.Object{whiteLabelPoolWithBootstrap(&apiv1.BootstrapConfiguration{Noop: &apiv1.BootstrapNoop{}}), availableCluster},
			sqlIdentity:  branchv1alpha1.SQLIdentityWhiteLabel,
			storageClass: "default-storage-class",
			image:        whiteLabelPool.Spec.ClusterSpec.ImageName,
			cpuRequest:   "2",
			memory:       "4Gi",
			storageSize:  "250Gi",
		},
		"white label pool without a bootstrap is skipped": {
			objects:      []client.Object{whiteLabelPoolWithBootstrap(nil), availableCluster},
			sqlIdentity:  branchv1alpha1.SQLIdentityWhiteLabel,
			storageClass: "default-storage-class",
			image:        whiteLabelPool.Spec.ClusterSpec.ImageName,
			cpuRequest:   "2",
			memory:       "4Gi",
			storageSize:  "250Gi",
		},
		"white label branch skips a legacy pool": {
			objects:      []client.Object{matchingPool, availableCluster},
			sqlIdentity:  branchv1alpha1.SQLIdentityWhiteLabel,
			storageClass: "default-storage-class",
			image:        whiteLabelPool.Spec.ClusterSpec.ImageName,
			cpuRequest:   "2",
			memory:       "4Gi",
			storageSize:  "250Gi",
		},
		"legacy branch skips a white label pool": {
			objects:      []client.Object{whiteLabelPool, availableCluster},
			storageClass: "default-storage-class",
			image:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
			cpuRequest:   "2",
			memory:       "4Gi",
			storageSize:  "250Gi",
		},
		"no pools at all": {
			objects:      nil,
			storageClass: "default-storage-class",
			image:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
			cpuRequest:   "2",
			memory:       "4Gi",
			storageSize:  "250Gi",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			k8sClient := newPoolTestClient(t, tt.objects...)

			gotPoolName, got, err := findPoolCluster(ctx, k8sClient, k8sClient, namespace,
				tt.sqlIdentity, tt.storageClass, tt.image, tt.cpuRequest, tt.memory, tt.storageSize)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			if tt.wantName == "" {
				require.Nil(t, got)
				require.Empty(t, gotPoolName)
			} else {
				require.NotNil(t, got)
				require.Equal(t, tt.wantName, got.Name)
				require.Equal(t, tt.wantPoolName, gotPoolName)
			}
		})
	}
}

func TestFindPoolClusterConcurrent(t *testing.T) {
	origTimeout := poolClusterWaitTimeout
	origInterval := poolClusterPollInterval
	poolClusterWaitTimeout = 100 * time.Millisecond
	poolClusterPollInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		poolClusterWaitTimeout = origTimeout
		poolClusterPollInterval = origInterval
	})

	const (
		namespace    = "xata-clusters"
		storageClass = "default-storage-class"
		image        = "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5"
		cpuRequest   = "2"
		memory       = "4Gi"
		storageSize  = "250Gi"
		concurrency  = 10
	)
	poolUID := types.UID("pool-uid-123")

	pool := &cpv1alpha1.ClusterPool{
		Name:      "test-pool",
		Namespace: namespace,
		UID:       poolUID,
		Spec: cpv1alpha1.ClusterPoolSpec{
			Clusters: 1,
			ClusterSpec: apiv1.ClusterSpec{
				ImageName: image,
				StorageConfiguration: apiv1.StorageConfiguration{
					StorageClass: new(storageClass),
				},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse(cpuRequest),
						corev1.ResourceMemory: resource.MustParse(memory),
					},
				},
			},
		},
	}

	cluster := &apiv1.Cluster{
		Name:      "pool-cluster-1",
		Namespace: namespace,
		OwnerReferences: []metav1.OwnerReference{
			{
				APIVersion: cpv1alpha1.GroupVersion.String(),
				Kind:       cpv1alpha1.ClusterPoolKind,
				UID:        poolUID,
				Name:       "test-pool",
				Controller: new(true),
			},
		},
		Status: apiv1.ClusterStatus{
			Phase:          apiv1.PhaseHealthy,
			ReadyInstances: 1,
		},
	}

	k8sClient := newPoolTestClient(t, pool, cluster)

	type result struct {
		poolName string
		cluster  *apiv1.Cluster
		err      error
	}
	results := make([]result, concurrency)

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range concurrency {
		wg.Go(func() {
			<-start
			poolName, c, err := findPoolCluster(context.Background(), k8sClient, k8sClient, namespace, branchv1alpha1.SQLIdentityLegacy, storageClass, image, cpuRequest, memory, storageSize)
			results[i] = result{poolName: poolName, cluster: c, err: err}
		})
	}
	close(start)
	wg.Wait()

	winners := 0
	for _, r := range results {
		require.NoError(t, r.err)
		if r.cluster != nil {
			winners++
			require.Equal(t, "pool-cluster-1", r.cluster.Name)
			require.Equal(t, "test-pool", r.poolName)
		}
	}
	require.Equal(t, 1, winners, "exactly one caller should win the claim")

	updated := &apiv1.Cluster{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), updated))
	require.Empty(t, updated.OwnerReferences, "winning cluster should have owner references cleared")
}

func TestOrphanCluster(t *testing.T) {
	ctx := context.Background()
	cluster := &apiv1.Cluster{
		Name:      "pool-cluster-1",
		Namespace: "xata-clusters",
		OwnerReferences: []metav1.OwnerReference{
			{
				UID:  types.UID("pool-uid"),
				Name: "test-pool",
			},
		},
	}

	k8sClient := newPoolTestClient(t, cluster)

	err := orphanCluster(ctx, k8sClient, cluster)
	require.NoError(t, err)

	updated := &apiv1.Cluster{}
	err = k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), updated)
	require.NoError(t, err)
	require.Empty(t, updated.OwnerReferences)
}

func TestSlotPoolName(t *testing.T) {
	tests := map[string]struct {
		input string
		want  string
	}{
		"standard pool": {
			input: "pg18-micro",
			want:  "pg18-micro-slot",
		},
		"already slot pool": {
			input: "pg18-micro-slot",
			want:  "pg18-micro-slot",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := slotPoolName(tt.input)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestFindHealthyClusterInPool(t *testing.T) {
	const namespace = "xata-clusters"
	poolUID := types.UID("pool-uid-123")

	pool := &cpv1alpha1.ClusterPool{
		Name:      "test-pool",
		Namespace: namespace,
		UID:       poolUID,
	}

	ownerRef := metav1.OwnerReference{
		APIVersion: cpv1alpha1.GroupVersion.String(),
		Kind:       cpv1alpha1.ClusterPoolKind,
		UID:        poolUID,
		Name:       "test-pool",
		Controller: new(true),
	}

	now := metav1.Now()

	clusterWithSize := func(name, size string) *apiv1.Cluster {
		cluster := poolClusterForTest()
		cluster.Name = name
		cluster.Spec.StorageConfiguration.Size = size
		return cluster
	}

	tests := map[string]struct {
		objects  []client.Object
		wantName string
		wantErr  bool
	}{
		"returns healthy cluster": {
			objects: []client.Object{pool, &apiv1.Cluster{
				Name: "healthy-1", Namespace: namespace,
				OwnerReferences: []metav1.OwnerReference{ownerRef},
				Status: apiv1.ClusterStatus{
					Phase:          apiv1.PhaseHealthy,
					ReadyInstances: 1,
				},
			}},
			wantName: "healthy-1",
		},
		"skips cluster being deleted": {
			objects: []client.Object{pool, &apiv1.Cluster{
				Name: "deleting-1", Namespace: namespace,
				OwnerReferences:   []metav1.OwnerReference{ownerRef},
				DeletionTimestamp: &now,
				Finalizers:        []string{"test-finalizer"},
				Status: apiv1.ClusterStatus{
					Phase:          apiv1.PhaseHealthy,
					ReadyInstances: 1,
				},
			}},
		},
		"skips unhealthy cluster": {
			objects: []client.Object{pool, &apiv1.Cluster{
				Name: "unhealthy-1", Namespace: namespace,
				OwnerReferences: []metav1.OwnerReference{ownerRef},
				Status:          apiv1.ClusterStatus{Phase: apiv1.PhaseWaitingForInstancesToBeActive},
			}},
		},
		"picks first healthy, skips deleting": {
			objects: []client.Object{
				pool,
				&apiv1.Cluster{
					Name: "deleting-1", Namespace: namespace,
					OwnerReferences:   []metav1.OwnerReference{ownerRef},
					DeletionTimestamp: &now,
					Finalizers:        []string{"test-finalizer"},
					Status: apiv1.ClusterStatus{
						Phase:          apiv1.PhaseHealthy,
						ReadyInstances: 1,
					},
				},
				&apiv1.Cluster{
					Name: "healthy-2", Namespace: namespace,
					OwnerReferences: []metav1.OwnerReference{ownerRef},
					Status: apiv1.ClusterStatus{
						Phase:          apiv1.PhaseHealthy,
						ReadyInstances: 1,
					},
				},
			},
			wantName: "healthy-2",
		},
		"skips cluster whose disk is larger than requested": {
			objects: []client.Object{pool, clusterWithSize("larger-1", "250Gi")},
		},
		"returns cluster whose disk equals the request": {
			objects:  []client.Object{pool, clusterWithSize("equal-1", "100Gi")},
			wantName: "equal-1",
		},
		"returns error for an invalid cluster disk size": {
			objects: []client.Object{pool, clusterWithSize("invalid-1", "not-a-size")},
			wantErr: true,
		},
		"no clusters at all": {
			objects: []client.Object{pool},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			k8sClient := newPoolTestClient(t, tt.objects...)

			got, err := findHealthyClusterInPool(ctx, k8sClient, k8sClient, namespace, pool, resource.MustParse("100Gi"))
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			if tt.wantName == "" {
				require.Nil(t, got)
			} else {
				require.NotNil(t, got)
				require.Equal(t, tt.wantName, got.Name)
			}
		})
	}
}

func TestWaitForClusterCache(t *testing.T) {
	t.Run("returns nil when cache is ready", func(t *testing.T) {
		ready := make(chan struct{})
		close(ready)
		svc := &ClustersService{clusterCacheOk: ready}
		require.NoError(t, svc.waitForClusterCache(context.Background()))
	})

	t.Run("returns error when context is cancelled", func(t *testing.T) {
		svc := &ClustersService{clusterCacheOk: make(chan struct{})}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, svc.waitForClusterCache(ctx), context.Canceled)
	})
}

func newPoolTestClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, apiv1.AddToScheme(scheme))
	require.NoError(t, cpv1alpha1.AddToScheme(scheme))

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithIndex(&apiv1.Cluster{}, clusterOwnerKey, func(obj client.Object) []string {
			owner := metav1.GetControllerOf(obj)
			if owner == nil {
				return nil
			}
			if owner.APIVersion != cpv1alpha1.GroupVersion.String() || owner.Kind != cpv1alpha1.ClusterPoolKind {
				return nil
			}
			return []string{owner.Name}
		}).
		Build()
}
