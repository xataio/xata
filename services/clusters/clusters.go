package clusters

import (
	"context"
	"fmt"
	"strings"

	"github.com/rs/zerolog/log"

	"xata/internal/envcfg"
	"xata/internal/o11y"
	"xata/internal/service"
	"xata/services/clusters/internal/connectors/cnpg"
	"xata/services/clusters/internal/connectors/openebs"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	barmanPluginApi "github.com/cloudnative-pg/plugin-barman-cloud/api/v1"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"

	clustersv1 "xata/gen/proto/clusters/v1"

	cpv1alpha1 "xata/proto/clusterpool-operator/api/v1alpha1"
	branchv1alpha1 "xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/reconciler/resources"

	ipfiltering "xata/services/clusters/internal/ipfiltering"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

const MaxStorageSizeGi = 1024

const clusterOwnerKey = ".metadata.ownerReferences[controller=true].name"

// Ensure clusters implements GRPCService interface.
var _ service.GRPCService = (*ClustersService)(nil)

type ClustersService struct {
	// fail to compile if the service does not implement all the methods
	clustersv1.UnsafeClustersServiceServer

	config Config

	// Connectors
	cnpgConnector    cnpg.Connector
	openebsConnector openebs.Connector

	// Kubernetes client
	kubeClient client.Client

	// Cached reader for indexed cluster lookups (backed by cache.Cache in production)
	clusterReader      client.Reader
	clusterCacheOk     chan struct{}
	clusterCacheCancel context.CancelFunc
}

// NewClustersService creates a new instance of the service.
func NewClustersService() *ClustersService {
	return &ClustersService{}
}

func (c *ClustersService) Name() string {
	return "clusters"
}

// ReadConfig implements service.Service.
func (c *ClustersService) ReadConfig(ctx context.Context) error {
	if err := envcfg.Read(&c.config); err != nil {
		return err
	}
	return c.config.Validate()
}

// Init implements service.Service.
func (c *ClustersService) Init(ctx context.Context) error {
	// Initialize CNPG connector
	cnpgConnector, err := cnpg.NewConnector(c.config.KubeConfig)
	if err != nil {
		return fmt.Errorf("cannot init cnpg client: %w", err)
	}
	c.cnpgConnector = cnpgConnector

	// Initialize OpenEBS connector
	openebsConnector, err := openebs.NewConnector(c.config.KubeConfig)
	if err != nil {
		return fmt.Errorf("cannot init openebs client: %w", err)
	}
	c.openebsConnector = openebsConnector

	// Create a new scheme and register Branch CRs
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return fmt.Errorf("cannot add clientgo scheme: %w", err)
	}
	if err := branchv1alpha1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("cannot add branch scheme: %w", err)
	}
	if err := apiv1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("cannot add cnpg scheme: %w", err)
	}
	if err := barmanPluginApi.AddToScheme(scheme); err != nil {
		return fmt.Errorf("cannot add barman plugin scheme: %w", err)
	}
	if err := cpv1alpha1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("cannot add clusterpool scheme: %w", err)
	}

	// Get Kubernetes configuration
	restConfig := ctrl.GetConfigOrDie()

	// Initialize Kubernetes client
	kubeClient, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("cannot init kubernetes client: %w", err)
	}
	c.kubeClient = kubeClient

	clusterCache, err := cache.New(restConfig, cache.Options{
		Scheme:            scheme,
		ByObject:          map[client.Object]cache.ByObject{&apiv1.Cluster{}: {}},
		DefaultNamespaces: map[string]cache.Config{c.config.ClustersNamespace: {}},
	})
	if err != nil {
		return fmt.Errorf("create cluster cache: %w", err)
	}

	if err := clusterCache.IndexField(ctx, &apiv1.Cluster{}, clusterOwnerKey,
		func(obj client.Object) []string {
			owner := metav1.GetControllerOf(obj)
			if owner == nil {
				return nil
			}
			if owner.APIVersion != cpv1alpha1.GroupVersion.String() || owner.Kind != "ClusterPool" {
				return nil
			}
			return []string{owner.Name}
		},
	); err != nil {
		return fmt.Errorf("index cluster owner field: %w", err)
	}

	cacheCtx, cacheCancel := context.WithCancel(context.Background()) //nolint:G118 // cache must outlive Init ctx
	c.clusterReader = clusterCache
	c.clusterCacheCancel = cacheCancel
	c.clusterCacheOk = make(chan struct{})
	go func() {
		_ = clusterCache.Start(cacheCtx) //nolint:errcheck
	}()
	go func() {
		if clusterCache.WaitForCacheSync(cacheCtx) {
			close(c.clusterCacheOk)
		}
	}()

	return nil
}

// Setup runs any setup steps needed for the service (ie DB migrations).
func (c *ClustersService) Setup(ctx context.Context) error {
	// this is a stateless service, nothing to setup
	return nil
}

// Close cleans up any resources used by the service.
func (c *ClustersService) Close(ctx context.Context) error {
	if c.clusterCacheCancel != nil {
		c.clusterCacheCancel()
	}
	return nil
}

func (c *ClustersService) waitForClusterCache(ctx context.Context) error {
	select {
	case <-c.clusterCacheOk:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// RegisterGRPCHandlers implements service.GRPCService.
func (c *ClustersService) RegisterGRPCHandlers(o *o11y.O, server *grpc.Server) {
	clustersv1.RegisterClustersServiceServer(server, c)
}

// CreatePostgresCluster creates a new Branch Custom Resource. The
// branch-operator will reconcile the CR and create the CNPG Cluster and all
// other necessary resources.
func (c *ClustersService) CreatePostgresCluster(ctx context.Context, req *clustersv1.CreatePostgresClusterRequest) (*clustersv1.CreatePostgresClusterResponse, error) {
	var parent *branchv1alpha1.Branch
	var err error

	// Retrieve the parent Branch, if any
	if req.GetParentId() != "" {
		parent, err = c.getBranch(ctx, req.GetParentId())
		if err != nil {
			return nil, k8sErrorToGRPCError(err)
		}
	}

	// Validate continuous backup source, if any
	if cb, ok := req.GetDataSource().(*clustersv1.CreatePostgresClusterRequest_ContinuousBackup); ok {
		clusterID := cb.ContinuousBackup.GetClusterId()
		if clusterID == "" {
			return nil, status.Errorf(codes.InvalidArgument, "continuous_backup.cluster_id is required")
		}

		// verify objectstore exists
		objectStore, err := c.getObjectStore(ctx, clusterID)
		if err != nil {
			return nil, k8sErrorToGRPCError(err)
		}

		// Validate that the objectstore status has a recovery window with FirstRecoverabilityPoint set
		recoveryWindow, hasRecoveryWindow := objectStore.Status.ServerRecoveryWindow[clusterID]
		if !hasRecoveryWindow || recoveryWindow.FirstRecoverabilityPoint.IsZero() {
			return nil, status.Errorf(codes.NotFound, "no continuous backup for source cluster %s", clusterID)
		}
	}

	// Build the Branch Custom Resource to be created
	branchBuilder := NewBranchBuilder().
		FromCreateClusterRequest(req).
		WithOverridesFromParent(parent).
		WithDefaultStorageSize(c.config.ClustersStorageRequest).
		WithDefaultStorageClass(c.config.ClustersStorageClass).
		WithDefaultVolumeSnapshotClass(c.config.ClustersVolumeSnapshotClass).
		WithDefaultNodeSelector(c.config.ClustersNodeSelector).
		WithPooler(c.config.EnablePooler).
		WithXataUtilsPreloadLibrary().
		WithMandatoryPostgresParameters()

	if req.GetUsePool() {
		// If use_pool is set, we look for a create pool matching the request.
		// If one is found, a cluster from that pool is selected and used to instantiate the branch.

		if err := c.waitForClusterCache(ctx); err != nil {
			return nil, fmt.Errorf("wait for cluster cache: %w", err)
		}

		branch := branchBuilder.Build()
		storageClass := ptr.Deref(branch.Spec.ClusterSpec.Storage.StorageClass, "")
		image := branch.Spec.ClusterSpec.Image
		cpuReq := branch.Spec.ClusterSpec.Resources.Requests.Cpu().String()
		// Use the original memory from the request config for pool matching,
		// not the reduced value from resourceRequirements (which subtracts
		// the pooler memory reservation).
		memReq := req.GetConfiguration().GetMemory()
		log.Ctx(ctx).Info().
			Str("storageClass", storageClass).
			Str("image", image).
			Str("cpu", cpuReq).
			Str("memory", memReq).
			Msg("looking for pool cluster")

		poolName, poolCluster, err := findPoolCluster(ctx, c.kubeClient, c.clusterReader, c.config.ClustersNamespace,
			storageClass, image, cpuReq, memReq,
		)
		if err != nil {
			return nil, fmt.Errorf("find pool cluster: %w", err)
		}
		log.Ctx(ctx).Info().
			Bool("found", poolCluster != nil).
			Str("poolName", poolName).
			Msg("pool cluster search result")
		if poolCluster != nil {
			if err := orphanCluster(ctx, c.kubeClient, poolCluster); err != nil {
				return nil, fmt.Errorf("orphan pool cluster: %w", err)
			}
			branchBuilder.WithClusterFromPool(poolCluster.Name, wakeupPoolName(poolName))
		}
	}

	branch := branchBuilder.Build()

	// Create the Branch CR
	if err := c.kubeClient.Create(ctx, branch); err != nil {
		return nil, k8sErrorToGRPCError(err)
	}

	return &clustersv1.CreatePostgresClusterResponse{
		Id:     req.GetId(),
		Status: "Creating",
	}, nil
}

// UpdatePostgresCluster updates an existing Branch CR spec.
func (c *ClustersService) UpdatePostgresCluster(ctx context.Context, req *clustersv1.UpdatePostgresClusterRequest) (*clustersv1.UpdatePostgresClusterResponse, error) {
	// Get the Branch CR to be updated
	branch, err := c.getBranch(ctx, req.GetId())
	if err != nil {
		return nil, k8sErrorToGRPCError(err)
	}

	if req.GetUpdateConfiguration().StorageSize != nil {
		currentSize := quantityGi(resource.MustParse(branch.Spec.ClusterSpec.Storage.Size))
		requestedSize := req.GetUpdateConfiguration().GetStorageSize()
		if requestedSize < currentSize {
			return nil, status.Errorf(codes.InvalidArgument, "storage size cannot be decreased (current: %dGi, requested: %dGi)", currentSize, requestedSize)
		}
		if requestedSize > MaxStorageSizeGi {
			return nil, status.Errorf(codes.InvalidArgument, "storage size cannot exceed %dGi (requested: %dGi)", MaxStorageSizeGi, requestedSize)
		}
	}

	// Build the updated Branch Custom Resource
	branch = NewBranchBuilder().
		FromExistingBranch(branch).
		WithUpdatesFrom(req).
		WithXataUtilsPreloadLibrary().
		WithMandatoryPostgresParameters().
		Build()

	// Update the Branch CR
	if err := c.kubeClient.Update(ctx, branch); err != nil {
		return nil, k8sErrorToGRPCError(err)
	}

	// Create the WakeupRequest if the update requires waking up a pool
	// hibernated branch
	if err := c.createWakeupRequest(ctx, branch, req); err != nil {
		return nil, k8sErrorToGRPCError(err)
	}

	return &clustersv1.UpdatePostgresClusterResponse{}, nil
}

// DeletePostgresCluster deletes a Branch CR
func (c *ClustersService) DeletePostgresCluster(ctx context.Context, req *clustersv1.DeletePostgresClusterRequest) (*clustersv1.DeletePostgresClusterResponse, error) {
	// Get the Branch CR to be deleted
	branch, err := c.getBranch(ctx, req.GetId())
	if err != nil {
		return nil, k8sErrorToGRPCError(err)
	}

	// Delete the Branch CR
	if err := c.kubeClient.Delete(ctx, branch); err != nil {
		return nil, k8sErrorToGRPCError(err)
	}

	return &clustersv1.DeletePostgresClusterResponse{}, nil
}

// DescribePostgresCluster retrieves a description of the Branch CR and its
// associated Cluster status.
func (c *ClustersService) DescribePostgresCluster(ctx context.Context, request *clustersv1.DescribePostgresClusterRequest) (*clustersv1.DescribePostgresClusterResponse, error) {
	// Get the Branch corresponding to the requested ID
	branch, err := c.getBranch(ctx, request.Id)
	if err != nil {
		return nil, k8sErrorToGRPCError(err)
	}

	// Default the cluster status to Healthy/Hibernated to handle:
	// * The branch has no associated cluster
	// * The branch has an associated cluster but the Cluster resource is not
	//   found, eg if Cluster reconciliation is waiting for a healthy parent
	clusterStatus := &clustersv1.ClusterStatus{
		Status:     apiv1.PhaseHealthy,
		StatusType: clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED,
	}

	// If the Branch has an associated Cluster, construct the status from the
	// live Cluster status
	if branch.HasClusterName() {
		cluster, err := c.getCluster(ctx, branch.ClusterName())
		if err == nil {
			clusterStatus = BuildClusterStatus(cluster)
		} else if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("getting cluster for branch: %w", err)
		}
	}

	// Build the ScaleToZero configuration from the Branch spec.
	scaleToZero := &clustersv1.ScaleToZero{}
	if branch.Spec.ClusterSpec.ScaleToZero != nil {
		scaleToZero.Enabled = branch.Spec.ClusterSpec.ScaleToZero.Enabled
		scaleToZero.InactivityPeriodMinutes = int64(branch.Spec.ClusterSpec.ScaleToZero.InactivityPeriodMinutes)
	}

	// Build the BackupConfiguration from the Branch spec.
	backupConfiguration := &clustersv1.BackupConfiguration{}
	if branch.Spec.BackupSpec != nil {
		backupConfiguration.BackupsEnabled = true
		if branch.Spec.BackupSpec.ScheduledBackup != nil {
			backupConfiguration.BackupSchedule = branch.Spec.BackupSpec.ScheduledBackup.Schedule
		}
		backupConfiguration.BackupRetention = branch.Spec.BackupSpec.Retention
	} else {
		backupConfiguration.BackupsEnabled = false
	}

	return &clustersv1.DescribePostgresClusterResponse{
		Id: branch.Name,
		Configuration: &clustersv1.ClusterConfiguration{
			NumInstances:                    branch.Spec.ClusterSpec.Instances,
			StorageSize:                     quantityGi(resource.MustParse(branch.Spec.ClusterSpec.Storage.Size)),
			ImageName:                       branch.Spec.ClusterSpec.Image,
			VcpuRequest:                     formatCPUResource(int(branch.Spec.ClusterSpec.Resources.Requests.Cpu().MilliValue())),
			VcpuLimit:                       formatCPUResource(int(branch.Spec.ClusterSpec.Resources.Limits.Cpu().MilliValue())),
			Memory:                          quantityGiStringWithPoolerReservation(*branch.Spec.ClusterSpec.Resources.Requests.Memory()),
			Hibernate:                       branch.Spec.ClusterSpec.Hibernation.IsEnabled(),
			ScaleToZero:                     scaleToZero,
			PostgresConfigurationParameters: resources.PostgresParametersToMap(branch.Spec.ClusterSpec.Postgres),
			PreloadLibraries:                branch.Spec.ClusterSpec.Postgres.SharedPreloadLibraries,
		},
		Status:              clusterStatus,
		BackupConfiguration: backupConfiguration,
	}, nil
}

// GetPostgresClusterCredentials retrieves the credentials for a Branch.
func (c *ClustersService) GetPostgresClusterCredentials(ctx context.Context, request *clustersv1.GetPostgresClusterCredentialsRequest) (*clustersv1.GetPostgresClusterCredentialsResponse, error) {
	creds, err := c.cnpgConnector.GetClusterCredentials(ctx, request.GetId(), c.config.ClustersNamespace, request.GetUsername())
	if err != nil {
		if strings.Contains(err.Error(), fmt.Sprintf("secrets \"%s\" not found", request.GetId()+"-"+request.GetUsername())) {
			return nil, SecretNotFoundForIDError(request.GetId())
		}
		return nil, fmt.Errorf("get credentials: %w", err)
	}

	return &clustersv1.GetPostgresClusterCredentialsResponse{
		Username: creds.Username,
		Password: creds.Password,
	}, nil
}

// RegisterPostgresCluster `registers` a Branch by creating a copy of its K8S
// services in the `xata-clusters` namespace. This RPC is intended to be
// invoked on the `clusters` service in the primary cell to register branches
// created on secondary cells, to enable cross-cell routing via Cilium
// ClusterMesh
func (c *ClustersService) RegisterPostgresCluster(ctx context.Context, request *clustersv1.RegisterPostgresClusterRequest) (*clustersv1.RegisterPostgresClusterResponse, error) {
	err := c.cnpgConnector.RegisterCluster(ctx, request.Id, c.config.ClustersNamespace, c.config.XataNamespace)
	if err != nil {
		return nil, fmt.Errorf("register: %w", err)
	}

	return &clustersv1.RegisterPostgresClusterResponse{}, nil
}

// DeregisterPostgresCluster removes the K8S service copies created by
// RegisterPostgresCluster
func (c *ClustersService) DeregisterPostgresCluster(ctx context.Context, request *clustersv1.DeregisterPostgresClusterRequest) (*clustersv1.DeregisterPostgresClusterResponse, error) {
	err := c.cnpgConnector.DeregisterCluster(ctx, request.Id, c.config.ClustersNamespace, c.config.XataNamespace)
	if err != nil {
		return nil, fmt.Errorf("deregister: %w", err)
	}

	return &clustersv1.DeregisterPostgresClusterResponse{}, nil
}

// GetCellUtilization returns the available storage space in the cell.
func (c *ClustersService) GetCellUtilization(ctx context.Context, request *clustersv1.GetCellUtilizationRequest) (*clustersv1.GetCellUtilizationResponse, error) {
	bytes, err := c.openebsConnector.AvailableSpaceBytes(ctx)
	if err != nil {
		return nil, fmt.Errorf("get cell utilization: %w", err)
	}

	return &clustersv1.GetCellUtilizationResponse{
		AvailableBytes: bytes,
	}, nil
}

// GetObjectStore retrieves the Barman ObjectStore status and recovery windows
// for a branch.
func (c *ClustersService) GetObjectStore(ctx context.Context, request *clustersv1.GetObjectStoreRequest) (*clustersv1.GetObjectStoreResponse, error) {
	objectStore, err := c.cnpgConnector.GetObjectStore(ctx, request.Id, c.config.ClustersNamespace)
	if err != nil {
		if strings.Contains(err.Error(), fmt.Sprintf("objectstores.barman.io \"%s\" not found", request.Id)) {
			return nil, fmt.Errorf("object store not found for cluster %s", request.Id)
		}
		return nil, fmt.Errorf("get object store status: %w", err)
	}

	response := &clustersv1.GetObjectStoreResponse{
		Status: &clustersv1.ObjectStoreStatus{
			ServerRecoveryWindow: make(map[string]*clustersv1.RecoveryWindow),
		},
	}

	for server, window := range objectStore.Status.ServerRecoveryWindow {
		recoveryWindow := &clustersv1.RecoveryWindow{}

		if window.FirstRecoverabilityPoint != nil {
			recoveryWindow.FirstRecoverabilityPoint = window.FirstRecoverabilityPoint.String()
		}
		if window.LastSuccessfulBackupTime != nil {
			recoveryWindow.LastSuccessfulBackupTime = window.LastSuccessfulBackupTime.String()
		}
		if window.LastFailedBackupTime != nil {
			recoveryWindow.LastFailedBackupTime = window.LastFailedBackupTime.String()
		}

		response.Status.ServerRecoveryWindow[server] = recoveryWindow
	}

	return response, nil
}

// SetBranchIPFiltering sets the IP filtering configuration for a branch in the ConfigMap.
func (c *ClustersService) SetBranchIPFiltering(ctx context.Context, request *clustersv1.SetBranchIPFilteringRequest) (*clustersv1.SetBranchIPFilteringResponse, error) {
	if request.BranchId == "" {
		return nil, fmt.Errorf("branch_id is required")
	}
	if request.IpFiltering == nil {
		return nil, fmt.Errorf("ip_filtering is required")
	}

	// Convert proto config to internal config
	config := ipfiltering.IPFilteringConfig{
		Enabled: request.IpFiltering.Enabled,
		Allowed: request.IpFiltering.Allowed,
	}

	// Set the configuration in the ConfigMap
	if err := ipfiltering.SetBranchIPFiltering(ctx, c.kubeClient, c.config.XataNamespace, request.BranchId, config); err != nil {
		return nil, fmt.Errorf("failed to set branch IP filtering: %w", err)
	}

	return &clustersv1.SetBranchIPFilteringResponse{}, nil
}

// SetBranchesIPFiltering sets the IP filtering configuration for multiple branches in the ConfigMap.
func (c *ClustersService) SetBranchesIPFiltering(ctx context.Context, request *clustersv1.SetBranchesIPFilteringRequest) (*clustersv1.SetBranchesIPFilteringResponse, error) {
	if len(request.BranchIds) == 0 {
		return nil, fmt.Errorf("at least one branch_id is required")
	}
	if request.IpFiltering == nil {
		return nil, fmt.Errorf("ip_filtering is required")
	}

	// Convert proto config to internal config
	config := ipfiltering.IPFilteringConfig{
		Enabled: request.IpFiltering.Enabled,
		Allowed: request.IpFiltering.Allowed,
	}

	// Set the configuration in the ConfigMap for all branches
	if err := ipfiltering.SetBranchesIPFiltering(ctx, c.kubeClient, c.config.XataNamespace, request.BranchIds, config); err != nil {
		return nil, fmt.Errorf("failed to set branches IP filtering: %w", err)
	}

	return &clustersv1.SetBranchesIPFilteringResponse{}, nil
}

// GetBranchIPFiltering retrieves the IP filtering configuration for a branch from the ConfigMap.
func (c *ClustersService) GetBranchIPFiltering(ctx context.Context, request *clustersv1.GetBranchIPFilteringRequest) (*clustersv1.GetBranchIPFilteringResponse, error) {
	if request.BranchId == "" {
		return nil, fmt.Errorf("branch_id is required")
	}

	// Get the configuration from the ConfigMap
	config, err := ipfiltering.GetBranchIPFiltering(ctx, c.kubeClient, c.config.XataNamespace, request.BranchId)
	if err != nil {
		return nil, fmt.Errorf("failed to get branch IP filtering: %w", err)
	}

	// Convert internal config to proto config
	return &clustersv1.GetBranchIPFilteringResponse{
		IpFiltering: &clustersv1.IPFilteringConfig{
			Enabled: config.Enabled,
			Allowed: config.Allowed,
		},
	}, nil
}

// DeleteBranchIPFiltering removes the IP filtering configuration entry for a branch from the ConfigMap.
func (c *ClustersService) DeleteBranchIPFiltering(ctx context.Context, request *clustersv1.DeleteBranchIPFilteringRequest) (*clustersv1.DeleteBranchIPFilteringResponse, error) {
	if request.BranchId == "" {
		return nil, fmt.Errorf("branch_id is required")
	}

	// Delete the configuration from the ConfigMap
	if err := ipfiltering.DeleteBranchIPFiltering(ctx, c.kubeClient, c.config.XataNamespace, request.BranchId); err != nil {
		return nil, fmt.Errorf("failed to delete branch IP filtering: %w", err)
	}

	return &clustersv1.DeleteBranchIPFilteringResponse{}, nil
}

// getBranch retrieves the Branch CR for the given branch ID.
func (c *ClustersService) getBranch(ctx context.Context, id string) (*branchv1alpha1.Branch, error) {
	branch := &branchv1alpha1.Branch{}
	err := c.kubeClient.Get(ctx, types.NamespacedName{Name: id}, branch)
	if err != nil {
		return nil, err
	}
	return branch, nil
}

// getCluster retrieves the Cluster CR for the given cluster ID.
func (c *ClustersService) getCluster(ctx context.Context, id string) (*apiv1.Cluster, error) {
	cluster := &apiv1.Cluster{}
	err := c.kubeClient.Get(ctx, types.NamespacedName{
		Name:      id,
		Namespace: c.config.ClustersNamespace,
	}, cluster)
	if err != nil {
		return nil, err
	}
	return cluster, nil
}

// getObjectStore retrieves a CNPG Barman ObjectStore by ID
func (c *ClustersService) getObjectStore(ctx context.Context, id string) (*barmanPluginApi.ObjectStore, error) {
	objectStore := &barmanPluginApi.ObjectStore{}
	err := c.kubeClient.Get(ctx, types.NamespacedName{
		Name:      id,
		Namespace: c.config.ClustersNamespace,
	}, objectStore)
	if err != nil {
		return nil, err
	}
	return objectStore, nil
}
