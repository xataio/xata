package branchingreplicaoperator

import (
	"context"

	"github.com/go-logr/zerologr"
	walgv1beta1 "github.com/wal-g/cnpg-plugin-wal-g/api/v1beta1"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"

	"xata/internal/envcfg"
	"xata/internal/o11y"
	"xata/internal/service"
	"xata/services/branching-replica-operator/api/v1alpha1"
	"xata/services/branching-replica-operator/pkg/reconciler"
)

var _ service.Service = (*BranchingReplicaOperatorService)(nil)

var _ service.RunnerService = (*BranchingReplicaOperatorService)(nil)

type BranchingReplicaOperatorService struct {
	config  Config
	manager ctrl.Manager
}

func NewBranchingReplicaOperatorService() *BranchingReplicaOperatorService {
	return &BranchingReplicaOperatorService{}
}

func (s *BranchingReplicaOperatorService) Name() string {
	return "branching-replica-operator"
}

func (s *BranchingReplicaOperatorService) ReadConfig(ctx context.Context) error {
	return envcfg.Read(&s.config)
}

func (s *BranchingReplicaOperatorService) Init(ctx context.Context) error {
	config := ctrl.GetConfigOrDie()

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
	if err := walgv1beta1.AddToScheme(scheme); err != nil {
		return err
	}

	mgr, err := ctrl.NewManager(config, ctrl.Options{
		Scheme: scheme,
		Cache: cache.Options{
			DefaultNamespaces: map[string]cache.Config{
				s.config.ClustersNamespace: {},
			},
		},
	})
	if err != nil {
		return err
	}

	r := &reconciler.BranchingReplicaReconciler{
		Client:           mgr.GetClient(),
		Tolerations:      s.config.Tolerations.Value,
		ImagePullSecrets: s.config.ImagePullSecrets,
	}
	if err := r.SetupWithManager(mgr); err != nil {
		return err
	}

	s.manager = mgr
	return nil
}

func (s *BranchingReplicaOperatorService) Setup(ctx context.Context) error {
	return nil
}

func (s *BranchingReplicaOperatorService) Close(ctx context.Context) error {
	return nil
}

func (s *BranchingReplicaOperatorService) Run(ctx context.Context, o *o11y.O) error {
	logger := o.Logger()

	ctrlLogger := logger.With().Str("module", "controller-runtime").Logger()
	ctrl.SetLogger(zerologr.New(&ctrlLogger))

	logger.Info().Msg("branching-replica-operator starting")

	if err := s.manager.Start(ctx); err != nil {
		logger.Error().Err(err).Msg("branching-replica-operator failed")
		return err
	}

	logger.Info().Msg("branching-replica-operator stopping")
	return nil
}
