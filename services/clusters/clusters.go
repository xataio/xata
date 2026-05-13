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
	"xata/services/clusters/observability"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
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

	// Observability queriers — nil when no per-cell backend is configured.
	// In that case GetBranchMetrics/GetBranchLogs return Unimplemented and
	// the projects service falls through to the legacy SigNoz path.
	metricsQuerier *observability.MetricsQuerier
	logsQuerier    *observability.LogsQuerier
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
	openebsConnector, err := openebs.NewConnector(c.config.KubeConfig, c.config.DiskPoolNamespace)
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
			if owner.APIVersion != cpv1alpha1.GroupVersion.String() || owner.Kind != cpv1alpha1.ClusterPoolKind {
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
		if !clusterCache.WaitForCacheSync(cacheCtx) {
			log.Ctx(cacheCtx).Error().Msg("cluster cache sync failed")
		}
		close(c.clusterCacheOk)
	}()

	if c.config.VictoriaMetricsURL != "" {
		vm, err := observability.NewVMClient(c.config.VictoriaMetricsURL, nil)
		if err != nil {
			return fmt.Errorf("init victoria-metrics client: %w", err)
		}
		c.metricsQuerier = observability.NewMetricsQuerier(vm, c.config.ClustersNamespace)
	}
	if c.config.VictoriaLogsURL != "" {
		c.logsQuerier = observability.NewLogsQuerier(
			observability.NewVLClient(c.config.VictoriaLogsURL, nil),
			c.config.ClustersNamespace,
		)
	}

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

	// Pick the default storage class. If the org has the UseXatastor flag set
	// and the cell has xatastor deployed, root clusters provision on xatastor.
	// Child branches always inherit from the parent (handled by
	// WithOverridesFromParent below) so we only apply the override for roots.
	storageClass := c.config.ClustersStorageClass
	if req.GetUseXatastor() && c.config.XatastorEnabled && parent == nil {
		storageClass = "xatastor"
		log.Ctx(ctx).Info().Str("branchID", req.GetId()).Msg("using xatastor storage class for root cluster")
	}

	// Build the Branch Custom Resource to be created
	branchBuilder := NewBranchBuilder(c.config.XVolChildStorageClass).
		FromCreateClusterRequest(req).
		WithOverridesFromParent(parent).
		WithDefaultStorageSize(c.config.ClustersStorageRequest).
		WithDefaultStorageClass(storageClass).
		WithDefaultVolumeSnapshotClass(c.config.ClustersVolumeSnapshotClass).
		WithDefaultNodeSelector(c.config.ClustersNodeSelector).
		WithPooler(c.config.EnablePooler).
		WithXataUtilsPreloadLibrary().
		WithMandatoryPostgresParameters()

	// If use_pool is set, we look for a create pool matching the request. If one
	// is found, a cluster from that pool is selected and used to instantiate the
	// branch. Child branches do not use clusters from the create pool - their
	// clusters are created via XVol cloning and then waking up a cluster using a
	// WakeupRequest.
	if req.GetUsePool() && parent == nil {
		if err := c.waitForClusterCache(ctx); err != nil {
			return nil, fmt.Errorf("wait for cluster cache: %w", err)
		}

		branch := branchBuilder.Build()
		storageClass := ptr.Deref(branch.Spec.ClusterSpec.Storage.StorageClass, "")
		image := branch.Spec.ClusterSpec.Image
		cpuReq := branch.Spec.ClusterSpec.Resources.Requests.Cpu().String()
		memReq := branch.Spec.ClusterSpec.Resources.Requests.Memory().String()
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
			branchBuilder.WithClusterFromPool(poolCluster.Name, slotPoolName(poolName))
		}
	}

	branch := branchBuilder.Build()

	// Create the Branch CR
	if err := c.kubeClient.Create(ctx, branch); err != nil {
		return nil, k8sErrorToGRPCError(err)
	}

	// Create the WakeupRequest CR if the new branch requires one
	if err := c.createWakeupRequestForNewBranch(ctx, branch); err != nil {
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
		requestedSize := req.GetUpdateConfiguration().GetStorageSize()
		if requestedSize > MaxStorageSizeGi {
			return nil, status.Errorf(codes.InvalidArgument, "storage size cannot exceed %dGi (requested: %dGi)", MaxStorageSizeGi, requestedSize)
		}
	}

	// Build the updated Branch Custom Resource
	branch = NewBranchBuilder(c.config.XVolChildStorageClass).
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
	if err := c.createWakeupRequestFromUpdateClusterRequest(ctx, branch, req); err != nil {
		return nil, k8sErrorToGRPCError(err)
	}

	return &clustersv1.UpdatePostgresClusterResponse{}, nil
}

// userToSecretSuffix maps Postgres usernames to their K8s secret suffixes.
var userToSecretSuffix = map[string]string{
	"xata":     "app",
	"postgres": "superuser",
}

// RotatePostgresClusterCredentials deletes the K8s secret for the given user,
// triggering the branch-operator to recreate it with a new password on the
// next reconciliation loop.
func (c *ClustersService) RotatePostgresClusterCredentials(ctx context.Context, req *clustersv1.RotatePostgresClusterCredentialsRequest) (*clustersv1.RotatePostgresClusterCredentialsResponse, error) {
	suffix, ok := userToSecretSuffix[req.GetUser()]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "unknown user %q", req.GetUser())
	}

	// Verify the branch exists
	if _, err := c.getBranch(ctx, req.GetId()); err != nil {
		return nil, k8sErrorToGRPCError(err)
	}

	// Deleting the secret triggers the branch-operator to recreate it with a
	// new password. The operator's reconcileSecret uses CreateOrUpdate which
	// only generates a password when the secret doesn't exist (len(Data) == 0).
	// Once recreated, CNPG picks up the change via the cnpg.io/reload label
	// and syncs the new password to PostgreSQL.
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      req.GetId() + "-" + suffix,
			Namespace: c.config.ClustersNamespace,
		},
	}
	if err := c.kubeClient.Delete(ctx, secret); err != nil {
		return nil, k8sErrorToGRPCError(err)
	}

	return &clustersv1.RotatePostgresClusterCredentialsResponse{}, nil
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

// GetBranchMetrics queries the cell's VictoriaMetrics instance for the named
// metric and aggregations. The branch-scope is enforced server-side as
// defense in depth even though the projects handler also validates instance
// prefixes.
func (c *ClustersService) GetBranchMetrics(ctx context.Context, request *clustersv1.GetBranchMetricsRequest) (*clustersv1.GetBranchMetricsResponse, error) {
	if c.metricsQuerier == nil {
		return nil, status.Errorf(codes.Unimplemented, "metrics backend not configured for this cell")
	}
	if request.GetBranchId() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "branch_id is required")
	}
	if request.GetStart() == nil || request.GetEnd() == nil {
		return nil, status.Errorf(codes.InvalidArgument, "start and end are required")
	}
	for _, inst := range request.GetInstances() {
		if !strings.HasPrefix(inst, request.GetBranchId()+"-") {
			return nil, status.Errorf(codes.InvalidArgument, "instance %q is not in branch %q", inst, request.GetBranchId())
		}
	}

	res, err := c.metricsQuerier.Query(ctx,
		request.GetBranchId(),
		request.GetMetric(),
		request.GetInstances(),
		request.GetAggregations(),
		request.GetStart().AsTime(),
		request.GetEnd().AsTime(),
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query metrics: %v", err)
	}

	resp := &clustersv1.GetBranchMetricsResponse{
		Start:  request.GetStart(),
		End:    request.GetEnd(),
		Metric: request.GetMetric(),
		Unit:   res.Unit,
		Series: make([]*clustersv1.MetricSeries, 0, len(res.Series)),
	}
	for _, s := range res.Series {
		series := &clustersv1.MetricSeries{
			Aggregation: s.Aggregation,
			InstanceId:  s.InstanceID,
			Values:      make([]*clustersv1.MetricValue, 0, len(s.Values)),
		}
		for _, v := range s.Values {
			series.Values = append(series.Values, &clustersv1.MetricValue{
				Timestamp: timestamppb.New(v.Timestamp),
				Value:     v.Value,
			})
		}
		resp.Series = append(resp.Series, series)
	}
	return resp, nil
}

// GetBranchLogs queries the cell's VictoriaLogs instance.
func (c *ClustersService) GetBranchLogs(ctx context.Context, request *clustersv1.GetBranchLogsRequest) (*clustersv1.GetBranchLogsResponse, error) {
	if c.logsQuerier == nil {
		return nil, status.Errorf(codes.Unimplemented, "logs backend not configured for this cell")
	}
	if request.GetBranchId() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "branch_id is required")
	}
	if request.GetStart() == nil || request.GetEnd() == nil {
		return nil, status.Errorf(codes.InvalidArgument, "start and end are required")
	}
	limit := int(request.GetLimit())
	if limit <= 0 {
		limit = 100
	}

	filters := make([]observability.LogFilter, 0, len(request.GetFilters()))
	for _, f := range request.GetFilters() {
		filters = append(filters, observability.LogFilter{
			Field:  f.GetField(),
			Op:     f.GetOp(),
			Values: f.GetValues(),
			Value:  f.GetValue(),
		})
	}

	res, err := c.logsQuerier.Query(ctx,
		request.GetBranchId(),
		request.GetStart().AsTime(),
		request.GetEnd().AsTime(),
		filters,
		limit,
		request.GetCursor(),
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query logs: %v", err)
	}

	resp := &clustersv1.GetBranchLogsResponse{
		Start: request.GetStart(),
		End:   request.GetEnd(),
		Logs:  make([]*clustersv1.LogEntry, 0, len(res.Entries)),
	}
	if res.NextCursor != "" {
		nc := res.NextCursor
		resp.NextCursor = &nc
	}
	for _, e := range res.Entries {
		entry := &clustersv1.LogEntry{
			Timestamp:  timestamppb.New(e.Timestamp),
			InstanceId: e.InstanceID,
			Message:    e.Message,
		}
		if e.Level != "" {
			lvl := e.Level
			entry.Level = &lvl
		}
		if e.Process != "" {
			p := e.Process
			entry.Process = &p
		}
		resp.Logs = append(resp.Logs, entry)
	}
	return resp, nil
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
