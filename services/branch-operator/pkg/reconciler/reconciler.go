package reconciler

import (
	"context"
	"fmt"
	"strconv"

	barmanPluginApi "github.com/cloudnative-pg/plugin-barman-cloud/api/v1"
	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	v1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"xata/services/branch-operator/api/v1alpha1"
)

// +kubebuilder:rbac:groups=xata.io,resources=branches,verbs=get;list;watch
// +kubebuilder:rbac:groups=xata.io,resources=branches/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=xata.io,resources=xvols,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=barmancloud.cnpg.io,resources=objectstores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=postgresql.cnpg.io,resources=scheduledbackups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=postgresql.cnpg.io,resources=clusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=postgresql.cnpg.io,resources=poolers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshots,verbs=get;list;watch;create;update;patch;delete

const (
	OperatorName                   = "branch-operator"
	ReconciliationPausedAnnotation = "xata.io/reconciliation-paused"
	ClusterOwnerKey                = ".metadata.ownerReferences[controller=true].name"
)

// BranchReconciler reconciles a Branch object
type BranchReconciler struct {
	client.Client
	Scheme            *runtime.Scheme
	ClustersNamespace string
	BackupsBucket     string
	BackupsEndpoint   string
	Tolerations       []v1.Toleration
	EnforceZone       bool
	ImagePullSecrets  []string
}

// Reconcile handles reconciliation for Branch resources
func (r *BranchReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.Log.WithName(OperatorName)

	log.Info("reconciling Branch", "namespacedName", req.NamespacedName)

	// Fetch the branch resource
	branch := &v1alpha1.Branch{}
	if err := r.Get(ctx, req.NamespacedName, branch); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Don't reconcile if the branch is being deleted
	if !branch.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Check if reconciliation is paused
	if isReconciliationPaused(branch) {
		err := r.setReadyConditionUnknown(ctx, branch, v1alpha1.ReconciliationPausedReason)
		if err != nil {
			log.Error(err, "setting Branch Ready condition to Unknown")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Ensure status conditions are initialized
	if err := r.ensureStatusConditions(ctx, branch); err != nil {
		log.Error(err, "ensuring status conditions")
		return ctrl.Result{}, err
	}

	// Set ObservedGeneration to the branch's current Generation
	if branch.Status.ObservedGeneration != branch.Generation {
		branch.Status.ObservedGeneration = branch.Generation
		if err := r.Status().Update(ctx, branch); err != nil {
			log.Error(err, "updating Branch status")
			return ctrl.Result{}, err
		}
	}

	// Defer setting status based on errors that occur during reconciliation
	var err error
	defer func() {
		r.setStatusConditionFromError(ctx, branch, err)
		r.setLastErrorStatus(ctx, branch, err)
	}()

	// Reconcile the ObjectStore for the branch
	_, err = r.reconcileObjectStore(ctx, branch)
	if err != nil {
		log.Error(err, "reconciling ObjectStore")
		return ctrl.Result{}, err
	}

	// Reconcile the VolumeSnapshot for the branch
	_, err = r.reconcileVolumeSnapshot(ctx, branch)
	if err != nil {
		log.Error(err, "reconciling VolumeSnapshot")
		return ctrl.Result{}, err
	}

	// Reconcile the superuser Secret for the branch
	_, err = r.reconcileSuperuserSecret(ctx, branch)
	if err != nil {
		log.Error(err, "reconciling superuser Secret")
		return ctrl.Result{}, err
	}

	// Reconcile the app (xata) user Secret for the branch
	_, err = r.reconcileAppSecret(ctx, branch)
	if err != nil {
		log.Error(err, "reconciling app Secret")
		return ctrl.Result{}, err
	}

	// Reconcile the CNPG Cluster for the branch
	err = r.reconcileCluster(ctx, branch)
	if err != nil {
		log.Error(err, "reconciling Cluster")
		return ctrl.Result{}, err
	}

	// Reconcile any XVols for the branch
	_, err = r.reconcileXVols(ctx, branch)
	if err != nil {
		log.Error(err, "reconciling XVols")
		return ctrl.Result{}, err
	}

	// Reconcile any other CNPG Clusters owned by the branch
	_, err = r.reconcileOwnedClusters(ctx, branch)
	if err != nil {
		log.Error(err, "deleting extra Clusters")
		return ctrl.Result{}, err
	}

	// Reconcile the additional services for the branch
	_, err = r.reconcileAdditionalServiceRW(ctx, branch)
	if err != nil {
		log.Error(err, "reconciling additional service rw")
		return ctrl.Result{}, err
	}
	_, err = r.reconcileAdditionalServiceR(ctx, branch)
	if err != nil {
		log.Error(err, "reconciling additional service r")
		return ctrl.Result{}, err
	}
	_, err = r.reconcileAdditionalServiceRO(ctx, branch)
	if err != nil {
		log.Error(err, "reconciling additional service ro")
	}

	// Reconcile the additional service for the Pooler
	_, err = r.reconcileAdditionalServicePooler(ctx, branch)
	if err != nil {
		log.Error(err, "reconciling additional service pooler")
		return ctrl.Result{}, err
	}

	// Reconcile the Pooler for the branch
	_, err = r.reconcilePooler(ctx, branch)
	if err != nil {
		log.Error(err, "reconciling Pooler")
		return ctrl.Result{}, err
	}

	// Reconcile the ScheduledBackup for the branch
	_, err = r.reconcileScheduledBackup(ctx, branch)
	if err != nil {
		log.Error(err, "reconciling ScheduledBackup")
		return ctrl.Result{}, err
	}

	// Reconcile the NetworkPolicy for the branch
	_, err = r.reconcileNetworkPolicy(ctx, branch)
	if err != nil {
		log.Error(err, "reconciling NetworkPolicy")
		return ctrl.Result{}, err
	}

	// Reconcile the clusters Service for the branch
	_, err = r.reconcileClustersService(ctx, branch)
	if err != nil {
		log.Error(err, "reconciling clusters Service")
		return ctrl.Result{}, err
	}

	// Update XVol status fields on the Branch
	if err = r.updateXVolStatus(ctx, branch); err != nil {
		log.Error(err, "updating XVol status")
		return ctrl.Result{}, err
	}

	// Set the Branch Ready condition to True
	err = r.setReadyConditionTrue(ctx, branch, v1alpha1.ResourcesReadyReason)
	if err != nil {
		log.Error(err, "setting Branch Ready condition to True")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager
func (r *BranchReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	if err := setupIndexers(ctx, mgr); err != nil {
		return fmt.Errorf("setup indexers: %w", err)
	}

	onGenerationChanged := builder.WithPredicates(predicate.GenerationChangedPredicate{})
	onAnnotationOrGenerationChanged := builder.WithPredicates(GenerationOrAnnotationChanged())
	onRelevantClusterChanges := builder.WithPredicates(ClusterPhaseOrGenerationOrAnnotationChanged)

	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Branch{}, onAnnotationOrGenerationChanged).
		Owns(&networkingv1.NetworkPolicy{}, onGenerationChanged).
		Owns(&v1.Service{}, onGenerationChanged).
		Owns(&v1.Secret{}, onGenerationChanged).
		Owns(&barmanPluginApi.ObjectStore{}, onGenerationChanged).
		Owns(&apiv1.ScheduledBackup{}, onGenerationChanged).
		Owns(&snapshotv1.VolumeSnapshot{}, onGenerationChanged).
		Owns(&apiv1.Pooler{}, onGenerationChanged).
		Owns(&apiv1.Cluster{}, onRelevantClusterChanges).
		Complete(r)
}

// setupIndexers sets up field indexers for efficient lookups of the Clusters
// owned by a Branch
func setupIndexers(ctx context.Context, mgr ctrl.Manager) error {
	return mgr.GetFieldIndexer().IndexField(ctx, &apiv1.Cluster{}, ClusterOwnerKey,
		func(obj client.Object) []string {
			owner := metav1.GetControllerOf(obj)
			if owner == nil {
				return nil
			}
			if owner.APIVersion != v1alpha1.GroupVersion.String() || owner.Kind != "Branch" {
				return nil
			}
			return []string{owner.Name}
		},
	)
}

// isReconciliationPaused checks if the Branch has the reconciliation paused
// annotation set to a truthy value
func isReconciliationPaused(branch *v1alpha1.Branch) bool {
	if branch.Annotations == nil {
		return false
	}

	// No annotation present - not paused
	value, exists := branch.Annotations[ReconciliationPausedAnnotation]
	if !exists {
		return false
	}

	// Check the value of the annotation
	paused, err := strconv.ParseBool(value)
	if err != nil {
		return false
	}

	return paused
}
