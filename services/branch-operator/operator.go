package branchoperator

import (
	"context"
	"fmt"

	barmanPluginApi "github.com/cloudnative-pg/plugin-barman-cloud/api/v1"
	"github.com/go-logr/zerologr"
	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	storage "google.golang.org/api/storage/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"xata/internal/envcfg"
	"xata/internal/o11y"
	"xata/internal/service"
	poolv1alpha1 "xata/proto/clusterpool-operator/api/v1alpha1"
	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/gcp/gcsfolder"
	"xata/services/branch-operator/pkg/reconciler"
	"xata/services/branch-operator/pkg/reconciler/metrics"
	"xata/services/branch-operator/pkg/wakeup"
)

var xvolGVK = schema.GroupVersionKind{
	Group:   "xata.io",
	Version: "v1alpha1",
	Kind:    "Xvol",
}

var clusterPoolGVK = poolv1alpha1.GroupVersion.WithKind(poolv1alpha1.ClusterPoolKind)

// Ensure BranchOperatorService implements Service interface
var _ service.Service = (*BranchOperatorService)(nil)

// Ensure BranchOperatorService implements RunnerService interface
var _ service.RunnerService = (*BranchOperatorService)(nil)

type BranchOperatorService struct {
	config           Config
	manager          ctrl.Manager
	branchReconciler *reconciler.BranchReconciler
	wakeupReconciler *wakeup.WakeupReconciler
}

// NewBranchOperatorService creates a new instance of the Branch operator service.
func NewBranchOperatorService() *BranchOperatorService {
	return &BranchOperatorService{}
}

func (s *BranchOperatorService) Name() string {
	return "branch-operator"
}

// ReadConfig implements service.Service.
func (s *BranchOperatorService) ReadConfig(ctx context.Context) error {
	return envcfg.Read(&s.config)
}

// Init implements service.Service.
func (s *BranchOperatorService) Init(ctx context.Context) error {
	// Get Kubernetes configuration
	config := ctrl.GetConfigOrDie()

	if s.config.KubeClientQPS > 0 {
		config.QPS = float32(s.config.KubeClientQPS)
	}
	if s.config.KubeClientBurst > 0 {
		config.Burst = s.config.KubeClientBurst
	}

	// Create a new scheme and register types
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return err
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		return err
	}
	if err := apiv1.AddToScheme(scheme); err != nil {
		return err
	}
	if err := barmanPluginApi.AddToScheme(scheme); err != nil {
		return err
	}
	if err := snapshotv1.AddToScheme(scheme); err != nil {
		return err
	}
	if err := poolv1alpha1.AddToScheme(scheme); err != nil {
		return err
	}

	podNamespaces := []string{s.config.ClustersNamespace}
	if s.config.XatastorEnabled {
		podNamespaces = append(podNamespaces, s.config.CSINodeNamespace)
	}

	cacheByObject := map[ctrlclient.Object]cache.ByObject{
		&corev1.Secret{}: {
			Namespaces: cacheNamespaces(s.config.ClustersNamespace),
		},
		&corev1.PersistentVolume{}: {},
		&corev1.PersistentVolumeClaim{}: {
			Namespaces: cacheNamespaces(s.config.ClustersNamespace),
		},
		&corev1.Pod{}: {
			Namespaces: cacheNamespaces(podNamespaces...),
		},
		&corev1.Service{}: {
			Namespaces: cacheNamespaces(s.config.ClustersNamespace),
		},
		&corev1.ServiceAccount{}: {
			Namespaces: cacheNamespaces(s.config.ClustersNamespace),
		},
		&networkingv1.NetworkPolicy{}: {
			Namespaces: cacheNamespaces(s.config.ClustersNamespace),
		},
		&barmanPluginApi.ObjectStore{}: {
			Namespaces: cacheNamespaces(s.config.ClustersNamespace),
		},
		&apiv1.Cluster{}: {
			Namespaces: cacheNamespaces(s.config.ClustersNamespace),
		},
		&apiv1.Pooler{}: {
			Namespaces: cacheNamespaces(s.config.ClustersNamespace),
		},
		&apiv1.ScheduledBackup{}: {
			Namespaces: cacheNamespaces(s.config.ClustersNamespace),
		},
		&snapshotv1.VolumeSnapshot{}: {
			Namespaces: cacheNamespaces(s.config.ClustersNamespace),
		},
		&v1alpha1.WakeupRequest{}: {
			Namespaces: cacheNamespaces(s.config.ClustersNamespace),
		},
	}
	if hasAPIResource(config, clusterPoolGVK) {
		cacheByObject[&poolv1alpha1.ClusterPool{}] = cache.ByObject{
			Namespaces: cacheNamespaces(s.config.ClustersNamespace),
		}
	}
	if hasAPIResource(config, xvolGVK) {
		cacheByObject[newXVolObject()] = cache.ByObject{}
	}

	// Create the controller manager
	mgr, err := ctrl.NewManager(config, ctrl.Options{
		Scheme: scheme,
		Cache: cache.Options{
			ByObject: cacheByObject,
		},
	})
	if err != nil {
		return err
	}

	// Create and setup the branch reconciler
	reconciler := &reconciler.BranchReconciler{
		Client:                               mgr.GetClient(),
		Scheme:                               mgr.GetScheme(),
		ClustersNamespace:                    s.config.ClustersNamespace,
		BackupsBucket:                        s.config.BackupsBucket,
		BackupsEndpoint:                      s.config.BackupsEndpoint,
		PgBackRestWebIdentityRoleARN:         s.config.PgBackRestWebIdentityRoleARN,
		PgBackRestWorkloadIdentityPool:       s.config.PgBackRestWorkloadIdentityPool,
		BackupsCredentialsSecretName:         s.config.BackupsCredentialsSecretName,
		BackupsCredentialsAccessKeyIDKey:     s.config.BackupsCredentialsAccessKeyIDKey,
		BackupsCredentialsSecretAccessKeyKey: s.config.BackupsCredentialsSecretAccessKeyKey,
		CloudProvider:                        s.config.CloudProvider,
		BarmanRegionSecretName:               s.config.BarmanRegionSecretName,
		BarmanRegionSecretKey:                s.config.BarmanRegionSecretKey,
		Tolerations:                          s.config.Tolerations.Value,
		EnforceZone:                          s.config.EnforceZone,
		ImagePullSecrets:                     s.config.ImagePullSecrets,
		MaxConcurrentReconciles:              s.config.BranchMaxConcurrent,
	}
	if err := reconciler.SetupWithManager(ctx, mgr); err != nil {
		return err
	}
	s.branchReconciler = reconciler

	if s.config.XatastorEnabled {
		wakeupReconciler := &wakeup.WakeupReconciler{
			Client:                  mgr.GetClient(),
			Scheme:                  mgr.GetScheme(),
			Recorder:                mgr.GetEventRecorder(wakeup.ReconcilerName),
			CSINodeNamespace:        s.config.CSINodeNamespace,
			CSINodePort:             s.config.CSINodePort,
			WakeupRequestTTL:        s.config.WakeupRequestTTL,
			WakeupRPCTimeout:        s.config.WakeupRPCTimeout,
			PasswordSyncTimeout:     s.config.WakeupPasswordSyncTimeout,
			MaxConcurrentReconciles: s.config.WakeupMaxConcurrent,
		}
		if err := wakeupReconciler.SetupWithManager(ctx, mgr); err != nil {
			return err
		}
		s.wakeupReconciler = wakeupReconciler
	}

	if s.config.PgBackRestWorkloadIdentityPool != "" && s.config.PgBackRestGCSProjectNumber != "" {
		if err := s.setupGCSFolderReconciler(ctx, mgr); err != nil {
			return err
		}
	}

	s.manager = mgr
	return nil
}

func (s *BranchOperatorService) setupGCSFolderReconciler(ctx context.Context, mgr ctrl.Manager) error {
	// Application Default Credentials: Workload Identity in GKE.
	gcs, err := storage.NewService(ctx)
	if err != nil {
		return fmt.Errorf("create GCS client: %w", err)
	}
	r := &gcsfolder.Reconciler{
		Client:                  mgr.GetClient(),
		Storage:                 gcs,
		ProjectNumber:           s.config.PgBackRestGCSProjectNumber,
		WorkloadIdentityPool:    s.config.PgBackRestWorkloadIdentityPool,
		MaxConcurrentReconciles: s.config.GCSFolderMaxConcurrent,
	}
	return r.SetupWithManager(mgr)
}

func cacheNamespaces(namespaces ...string) map[string]cache.Config {
	configs := make(map[string]cache.Config, len(namespaces))
	for _, namespace := range namespaces {
		configs[namespace] = cache.Config{}
	}
	return configs
}

func newXVolObject() *unstructured.Unstructured {
	xvol := &unstructured.Unstructured{}
	xvol.SetGroupVersionKind(xvolGVK)
	return xvol
}

func hasAPIResource(config *rest.Config, gvk schema.GroupVersionKind) bool {
	client, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		return false
	}
	resourceList, err := client.ServerResourcesForGroupVersion(gvk.GroupVersion().String())
	if err != nil {
		return false
	}
	for _, resource := range resourceList.APIResources {
		if resource.Kind == gvk.Kind {
			return true
		}
	}
	return false
}

// Setup implements service.Service.
func (s *BranchOperatorService) Setup(ctx context.Context) error {
	// This is a stateless service, nothing to setup
	return nil
}

// Close implements service.Service.
func (s *BranchOperatorService) Close(ctx context.Context) error {
	return nil
}

// Run implements service.RunnerService.
func (s *BranchOperatorService) Run(ctx context.Context, o *o11y.O) error {
	logger := o.Logger()

	// Set up controller-runtime logger
	ctrlLogger := logger.With().Str("module", "controller-runtime").Logger()
	ctrl.SetLogger(zerologr.New(&ctrlLogger))

	// Set up the metrics and the observability of the reconcilers
	branchMetrics, err := metrics.New(o.Meter(metrics.MeterName))
	if err != nil {
		return fmt.Errorf("create branch reconciler metrics: %w", err)
	}
	s.branchReconciler.Metrics = branchMetrics
	if s.wakeupReconciler != nil {
		s.wakeupReconciler.O = o
	}

	logger.Info().Msg("branch-operator starting")

	// Start the controller manager
	if err := s.manager.Start(ctx); err != nil {
		logger.Error().Err(err).Msg("branch-operator failed")
		return err
	}

	logger.Info().Msg("branch-operator stopping")
	return nil
}
