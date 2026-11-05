package reconciler

import (
	"context"
	"fmt"
	"strconv"
	"time"

	barmanPluginApi "github.com/cloudnative-pg/plugin-barman-cloud/api/v1"
	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	v1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/reconciler/metrics"
)

// +kubebuilder:rbac:groups=xata.io,resources=branches,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=xata.io,resources=branches/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles;clusterrolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete,namespace=xata-clusters
// +kubebuilder:rbac:groups=xata.io,resources=xvols,verbs=get;list;watch;create;patch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch,namespace=xata-clusters
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch,namespace=xata-clusters
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch,namespace=xata-clusters
// +kubebuilder:rbac:groups="",resources=serviceaccounts/token,verbs=create,namespace=xata-clusters
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch,namespace=xata-clusters
// +kubebuilder:rbac:groups=barmancloud.cnpg.io,resources=objectstores,verbs=get;list;watch;create;update;patch;delete,namespace=xata-clusters
// +kubebuilder:rbac:groups=postgresql.cnpg.io,resources=scheduledbackups,verbs=get;list;watch;create;update;patch;delete,namespace=xata-clusters
// +kubebuilder:rbac:groups=postgresql.cnpg.io,resources=clusters,verbs=get;list;watch;create;update;patch;delete,namespace=xata-clusters
// +kubebuilder:rbac:groups=postgresql.cnpg.io,resources=poolers,verbs=get;list;watch;create;update;patch;delete,namespace=xata-clusters
// +kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshots,verbs=get;list;watch;create;update;patch;delete,namespace=xata-clusters

const (
	OperatorName                   = "branch-operator"
	ReconciliationPausedAnnotation = "xata.io/reconciliation-paused"
	ClusterOwnerKey                = ".metadata.ownerReferences[controller=true].name"
)

const (
	cloudProviderAWS = "aws"
	cloudProviderGCP = "gcp"
)

// BranchReconciler reconciles a Branch object
type BranchReconciler struct {
	client.Client
	Metrics                              *metrics.Metrics
	Scheme                               *runtime.Scheme
	ClustersNamespace                    string
	BackupsBucket                        string
	BackupsEndpoint                      string
	PgBackRestWebIdentityRoleARN         string
	PgBackRestWorkloadIdentityPool       string
	BackupsCredentialsSecretName         string
	BackupsCredentialsAccessKeyIDKey     string
	BackupsCredentialsSecretAccessKeyKey string
	CloudProvider                        string
	BarmanRegionSecretName               string
	BarmanRegionSecretKey                string
	Tolerations                          []v1.Toleration
	EnforceZone                          bool
	ImagePullSecrets                     []string
	MaxConcurrentReconciles              int
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
		setReadyCondition(branch, metav1.ConditionUnknown, v1alpha1.ReconciliationPausedReason)
		if err := r.applyStatus(ctx, branch); err != nil {
			log.Error(err, "setting Branch Ready condition to Unknown")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Set the observed generation to the current generation
	branch.Status.ObservedGeneration = branch.Generation

	var err error
	defer func() {
		setStatusConditionFromError(branch, err)
		branch.Status.LastError = errorMessage(err)
		if applyErr := r.applyStatus(ctx, branch); applyErr != nil {
			log.Error(applyErr, "updating Branch status")
		}
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

	// Reconcile the XVol clone for the branch
	_, err = r.reconcileXVolClone(ctx, branch)
	if err != nil {
		log.Error(err, "reconciling cloned XVol")
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

	// Reconcile the pgBackRest cipher Secret for the branch
	_, err = r.reconcilePgBackRestCipher(ctx, branch)
	if err != nil {
		log.Error(err, "reconciling pgbackrest cipher Secrets")
		return ctrl.Result{}, err
	}

	// Reconcile the CNPG Cluster for the branch
	err = r.reconcileCluster(ctx, branch)
	if err != nil {
		log.Error(err, "reconciling Cluster")
		return ctrl.Result{}, err
	}

	// Update XVol status fields on the Branch
	if err = r.updateXVolStatus(ctx, branch); err != nil {
		log.Error(err, "updating XVol status")
		return ctrl.Result{}, err
	}

	// Update backup status fields on the Branch
	if err = r.updateBackupStatus(ctx, branch); err != nil {
		log.Error(err, "updating backup status")
		return ctrl.Result{}, err
	}

	// Reconcile XVols owner references for the branch
	_, err = r.reconcileXVolOwnership(ctx, branch)
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
		return ctrl.Result{}, err
	}

	// Reconcile the additional service for the Pooler
	_, err = r.reconcileAdditionalServicePooler(ctx, branch)
	if err != nil {
		log.Error(err, "reconciling additional service pooler")
		return ctrl.Result{}, err
	}

	// Reconcile the Pooler for the branch
	err = r.reconcilePooler(ctx, branch)
	if err != nil {
		log.Error(err, "reconciling Pooler")
		return ctrl.Result{}, err
	}

	// Reconcile the ScheduledBackup for the branch
	err = r.reconcileScheduledBackup(ctx, branch)
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

	// Reconcile RBAC resources for the scale to zero sidecar for the branch
	err = r.reconcileScaleToZeroRBAC(ctx, branch)
	if err != nil {
		log.Error(err, "reconciling scale to zero RBAC")
		return ctrl.Result{}, err
	}

	// Reconcile the pgBackRest ServiceAccount and token for the branch
	start := time.Now()
	tokenRefreshIn, err := r.reconcilePgBackRestIdentity(ctx, branch)
	r.Metrics.RecordPgBackRestIdentity(ctx, time.Since(start), err == nil)
	if err != nil {
		log.Error(err, "reconciling pgbackrest identity")
		return ctrl.Result{}, err
	}

	// Set the Branch Ready condition to True
	setReadyCondition(branch, metav1.ConditionTrue, v1alpha1.ResourcesReadyReason)

	return ctrl.Result{RequeueAfter: tokenRefreshIn}, nil
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
		Owns(&v1.ServiceAccount{}, onGenerationChanged).
		Owns(&barmanPluginApi.ObjectStore{}, onGenerationChanged).
		Owns(&apiv1.ScheduledBackup{}, onGenerationChanged).
		Owns(&snapshotv1.VolumeSnapshot{}, onGenerationChanged).
		Owns(&apiv1.Cluster{}, onRelevantClusterChanges).
		Owns(&rbacv1.ClusterRole{}, onGenerationChanged).
		Owns(&rbacv1.ClusterRoleBinding{}, onGenerationChanged).
		WithOptions(controller.Options{MaxConcurrentReconciles: r.MaxConcurrentReconciles}).
		Complete(r)
}

// setupIndexers sets up a field indexer for efficient lookups of the Clusters
// owned by a Branch
func setupIndexers(ctx context.Context, mgr ctrl.Manager) error {
	branchOwnerName := func(obj client.Object) []string {
		owner := metav1.GetControllerOf(obj)
		if owner == nil {
			return nil
		}
		if owner.APIVersion != v1alpha1.GroupVersion.String() || owner.Kind != v1alpha1.BranchKind {
			return nil
		}
		return []string{owner.Name}
	}

	return mgr.GetFieldIndexer().IndexField(ctx, &apiv1.Cluster{}, ClusterOwnerKey, branchOwnerName)
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
