package wakeup

import (
	"context"
	"fmt"
	"time"

	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	"go.opentelemetry.io/otel/trace"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"xata/internal/o11y"
	poolv1alpha1 "xata/proto/clusterpool-operator/api/v1alpha1"
	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/shared"
	"xata/services/branch-operator/pkg/wakeup/tracing"
)

const (
	PoolClusterOwnerKey = ".metadata.ownerReferences[controller=true,kind=ClusterPool].name"
	ReconcilerName      = "wakeup-reconciler"
)

// +kubebuilder:rbac:groups=xata.io,resources=wakeuprequests,verbs=get;list;watch;delete,namespace=xata-clusters
// +kubebuilder:rbac:groups=xata.io,resources=wakeuprequests/status,verbs=get;update;patch,namespace=xata-clusters
// +kubebuilder:rbac:groups=xata.io,resources=branches,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=xata.io,resources=clusterpools,verbs=get;list;watch,namespace=xata-clusters
// +kubebuilder:rbac:groups=xata.io,resources=xvols,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch,namespace=xata-clusters
// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch,namespace=xata-clusters
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// WakeupReconciler reconciles a WakeupRequest object
type WakeupReconciler struct {
	client.Client
	O                       *o11y.O
	Scheme                  *runtime.Scheme
	Recorder                events.EventRecorder
	CSINodeNamespace        string
	CSINodePort             int
	WakeupRequestTTL        time.Duration
	WakeupRPCTimeout        time.Duration
	PasswordSyncTimeout     time.Duration
	MaxConcurrentReconciles int
}

// Reconcile handles reconciliation for WakeupRequest resources
func (r *WakeupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	ctx = r.O.WithContext(ctx)
	log := ctrl.Log.WithName(ReconcilerName)
	start := time.Now()

	// Start a new root span for each reconcile
	ctx, span := tracing.Tracer(ctx).Start(ctx, tracing.SpanReconcile,
		trace.WithAttributes(tracing.AttrWakeupRequest.String(req.Name)))
	defer span.End()

	// Add the span's trace and span IDs to the logger so that log lines can be
	// correlated with the trace
	log = tracing.WithSpanIDs(log, span)

	log.Info("reconciling WakeupRequest", "namespacedName", req.NamespacedName)

	// Fetch the WakeupRequest resource
	wr := &v1alpha1.WakeupRequest{}
	if err := r.Get(ctx, req.NamespacedName, wr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	span.SetAttributes(
		tracing.AttrBranch.String(wr.Spec.BranchName),
		tracing.AttrXVol.String(wr.Spec.XVolName),
		tracing.AttrPasswordSync.String(string(wr.Spec.PasswordSync)),
	)

	// Don't reconcile if the WakeupRequest is being deleted
	if !wr.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Ensure status conditions are initialized
	if err := r.ensureStatusConditions(ctx, wr); err != nil {
		log.Error(err, "ensuring status conditions")
		return ctrl.Result{}, err
	}

	// Skip reconciliation if the WakeupRequest has succeeded and delete it if
	// its TTL has expired
	if r.isWakeupSucceeded(wr) {
		return r.deleteAfterTTL(ctx, wr)
	}

	// Skip reconciliation if the WakeupRequest has terminally failed
	if r.isWakeupFailed(wr) {
		return ctrl.Result{}, nil
	}

	// Set ObservedGeneration to the WakeupRequest's current Generation
	if wr.Status.ObservedGeneration != wr.Generation {
		wr.Status.ObservedGeneration = wr.Generation
		if err := r.Status().Update(ctx, wr); err != nil {
			log.Error(err, "updating WakeupRequest status")
			return ctrl.Result{}, err
		}
	}

	// Defer setting status based on errors that occur during reconciliation,
	// then log the outcome and duration
	var err error
	defer func() {
		ctx := context.WithoutCancel(ctx)

		o11y.RecordSpanResult(span, err)
		r.recordFailureEvent(wr, err)
		r.setStatusConditionFromError(ctx, wr, err)
		r.setLastErrorStatus(ctx, wr, err)

		if err != nil {
			log.Error(err, "reconciling WakeupRequest", "namespacedName", req.NamespacedName, "durationMs", time.Since(start).Milliseconds())
			return
		}
		log.Info("reconciled WakeupRequest", "namespacedName", req.NamespacedName, "durationMs", time.Since(start).Milliseconds())
	}()

	// Set the WakeupRequest status to InProgress. This ensures that we get an
	// optimistic concurrency error here if the WakeupRequest read from the cache
	// is stale and has actually already succeeded or failed
	err = r.setSucceededCondition(ctx, wr, metav1.ConditionUnknown, v1alpha1.WakeupInProgressReason)
	if err != nil {
		log.Error(err, "setting InProgress status")
		return ctrl.Result{}, err
	}

	// Fetch the branch to be woken up
	branch, err := r.getBranch(ctx, wr.Spec.BranchName)
	if err != nil {
		log.Error(err, "fetching Branch", "branchName", wr.Spec.BranchName)
		return ctrl.Result{}, ignoreTerminal(err)
	}

	// If the branch doesn't need to be woken up (ie it already has a cluster
	// assigned) then:
	// * ensure that the awaiting wakeup annotation is cleared by re-assigning
	//   the cluster to the branch
	// * set the Succeeded condition to True
	// * requeue the WakeupRequest for deletion after its TTL expires
	if branch.Spec.ClusterSpec.Name != nil {
		err = r.assignClusterToBranch(ctx, branch, *branch.Spec.ClusterSpec.Name, branch.Spec.ClusterSpec.Image)
		if err != nil {
			log.Error(err, "re-assigning cluster to Branch", "branchName", branch.Name)
			return ctrl.Result{}, ignoreTerminal(err)
		}

		err = r.setSucceededCondition(ctx, wr, metav1.ConditionTrue, v1alpha1.WakeupSucceededReason)
		if err != nil {
			return ctrl.Result{}, ignoreTerminal(err)
		}
		return ctrl.Result{RequeueAfter: r.WakeupRequestTTL}, nil
	}

	// Validate that the XVol is in a state that can be used as the target of a
	// wakeup operation
	err = r.validateXVolStatus(ctx, wr.Spec.XVolName)
	if err != nil {
		log.Error(err, "validating XVol status", "xvolName", wr.Spec.XVolName)
		return ctrl.Result{}, ignoreTerminal(err)
	}

	// Get the pool name from the Branch annotation
	poolName, err := r.wakeupPoolName(branch)
	if err != nil {
		log.Error(err, "getting pool name", "branchName", branch.Name)
		return ctrl.Result{}, ignoreTerminal(err)
	}

	// Remove a cluster from the wakeup pool
	cluster, err := r.takeClusterFromPool(ctx, wr.Namespace, poolName)
	if err != nil {
		log.Error(err, "taking cluster from pool", "poolName", poolName)
		return ctrl.Result{}, ignoreTerminal(err)
	}
	span.SetAttributes(tracing.AttrCluster.String(cluster.Name))

	// At this point we have taken a Cluster from the pool; if any subsequent
	// step fails we would have an orphaned Cluster. Ensure that if any error
	// occurs from this point forward, we attempt to clean up by deleting the
	// Cluster that was taken from the pool.
	defer func() {
		if err != nil {
			r.Delete(context.WithoutCancel(ctx), cluster)
		}
	}()

	// Update the Cluster's spec to set the login role password secret
	if wr.Spec.PasswordSync == v1alpha1.PasswordSyncModeWait {
		err = r.setUserPasswordSecret(ctx, branch, cluster)
		if err != nil {
			log.Error(err, "setting Cluster password secrets", "clusterName", cluster.Name)
			return ctrl.Result{}, ignoreTerminal(err)
		}
	}

	// Get the PV backing the Cluster's primary instance
	pv, err := r.getPersistentVolume(ctx, cluster)
	if err != nil {
		log.Error(err, "getting PV for cluster", "clusterName", cluster.Name)
		return ctrl.Result{}, ignoreTerminal(err)
	}

	// Find the CSI node plugin pod on the same node as the primary
	csiNodePod, err := r.getCSINodePod(ctx, cluster)
	if err != nil {
		log.Error(err, "getting CSI node pod for cluster", "clusterName", cluster.Name)
		return ctrl.Result{}, ignoreTerminal(err)
	}

	// Call the WakeUp RPC on the CSI node pod
	err = r.wakeUp(ctx, csiNodePod, wr.Spec.XVolName, pv.Name)
	if err != nil {
		log.Error(err, "calling WakeUp RPC", "clusterName", cluster.Name)
		return ctrl.Result{}, ignoreTerminal(err)
	}

	// Wait for the login role password secret to be synced to the Cluster
	if wr.Spec.PasswordSync == v1alpha1.PasswordSyncModeWait {
		var outcome string
		outcome, err = r.waitForRolePasswordSync(ctx, log, branch.Name, shared.BranchSQLIdentity(branch).Login, cluster)
		span.SetAttributes(tracing.AttrPasswordSyncOutcome.String(outcome))
		if err != nil {
			log.Error(err, "waiting for Cluster password secrets", "clusterName", cluster.Name)
			return ctrl.Result{}, ignoreTerminal(err)
		}
	}

	// Annotate the PV with the name of the XVol used to wake it up
	err = r.annotatePVWithXVol(ctx, pv, wr.Spec.XVolName)
	if err != nil {
		log.Error(err, "annotating PV with XVol name", "pvName", pv.Name)
		return ctrl.Result{}, ignoreTerminal(err)
	}

	// Adopt the pool cluster's image when it is a newer minor of the branch's
	// image, so that a pool version bump propagates to the branch on its next
	// wakeup instead of the branch reconciler rolling the warm cluster back to
	// the branch's older image right after adoption
	image := adoptableImage(branch.Spec.ClusterSpec.Image, cluster.Spec.ImageName)
	if image != "" {
		log.Info("adopting pool cluster image", "branchName", branch.Name,
			"branchImage", branch.Spec.ClusterSpec.Image, "clusterImage", image)
	} else {
		image = branch.Spec.ClusterSpec.Image
	}

	// Assign the cluster to the branch
	err = r.assignClusterToBranch(ctx, branch, cluster.Name, image)
	if err != nil {
		log.Error(err, "assigning cluster to Branch", "branchName", branch.Name)
		return ctrl.Result{}, ignoreTerminal(err)
	}

	// Set the WakeupRequest Succeeded condition to True
	err = r.setSucceededCondition(ctx, wr, metav1.ConditionTrue, v1alpha1.WakeupSucceededReason)
	if err != nil {
		log.Error(err, "setting Succeeded condition to True")
		return ctrl.Result{}, ignoreTerminal(err)
	}

	return ctrl.Result{RequeueAfter: r.WakeupRequestTTL}, nil
}

// SetupWithManager sets up the controller with the Manager
func (r *WakeupReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	if err := setupIndexers(ctx, mgr); err != nil {
		return fmt.Errorf("setup indexers: %w", err)
	}

	onGenerationChanged := builder.WithPredicates(predicate.GenerationChangedPredicate{})

	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.WakeupRequest{}, onGenerationChanged).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: r.MaxConcurrentReconciles,
			RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[ctrl.Request](
				5*time.Millisecond, 300*time.Second,
			),
		}).
		Complete(r)
}

// setupIndexers registers a field indexer on CNPG Cluster objects to ensure
// that Clusters owned by a ClusterPool can be efficiently looked up by pool
// name. Without this, claiming a Cluster from a pool would require listing all
// clusters in the namespace and filtering in memory
func setupIndexers(ctx context.Context, mgr ctrl.Manager) error {
	return mgr.GetFieldIndexer().IndexField(
		ctx, &apiv1.Cluster{}, PoolClusterOwnerKey,
		func(obj client.Object) []string {
			owner := metav1.GetControllerOf(obj)
			if owner == nil {
				return nil
			}
			if owner.APIVersion != poolv1alpha1.GroupVersion.String() || owner.Kind != poolv1alpha1.ClusterPoolKind {
				return nil
			}
			return []string{owner.Name}
		},
	)
}
