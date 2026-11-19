// Package gcsfolder gives the pgBackRest ServiceAccount of each Branch access
// to its own managed folder in the GCS backup bucket of the Branch.
package gcsfolder

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"google.golang.org/api/googleapi"
	storage "google.golang.org/api/storage/v1"

	"xata/internal/pgbackrest"
)

const (
	ReconcilerName = "gcs-folder"

	// Finalizer keeps the ServiceAccount until its managed folder is deleted.
	Finalizer = "xata.io/gcs-managed-folder"

	// ObservedFolderAnnotation records the last provisioned gs://bucket/folder/.
	ObservedFolderAnnotation = "xata.io/gcs-managed-folder-observed"

	// Role is the role of the ServiceAccount on its managed folder.
	Role = "roles/storage.objectUser"

	// ObservedRestoreFolderAnnotation records the gs://bucket/folder/ where
	// RestoreRole was last granted.
	ObservedRestoreFolderAnnotation = "xata.io/restore-gcs-managed-folder-observed"

	// RestoreRole is the role of the ServiceAccount on the managed folder it
	// restores from.
	RestoreRole = "roles/storage.objectViewer"
)

type Reconciler struct {
	Client  client.Client
	Storage *storage.Service

	// ProjectNumber and WorkloadIdentityPool identify the Workload Identity
	// pool of the cluster, for example "123456" and "my-project.svc.id.goog".
	ProjectNumber        string
	WorkloadIdentityPool string

	MaxConcurrentReconciles int
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	hasFolder := predicate.NewPredicateFuncs(func(o client.Object) bool {
		a := o.GetAnnotations()
		return a[pgbackrest.GCSFolderAnnotation] != "" || controllerutil.ContainsFinalizer(o, Finalizer)
	})

	return ctrl.NewControllerManagedBy(mgr).
		Named(ReconcilerName).
		For(&corev1.ServiceAccount{}, builder.WithPredicates(hasFolder)).
		WithOptions(controller.Options{MaxConcurrentReconciles: r.MaxConcurrentReconciles}).
		Complete(r)
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	sa := &corev1.ServiceAccount{}
	if err := r.Client.Get(ctx, req.NamespacedName, sa); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	var bucket, folder, previousBucket, previousFolder string
	var err error
	if location := sa.Annotations[pgbackrest.GCSFolderAnnotation]; location != "" {
		bucket, folder, err = pgbackrest.ParseGCSFolderURI(location)
		if err != nil {
			return ctrl.Result{}, err
		}
	}
	if location := sa.Annotations[ObservedFolderAnnotation]; location != "" {
		previousBucket, previousFolder, err = pgbackrest.ParseGCSFolderURI(location)
		if err != nil {
			return ctrl.Result{}, err
		}
	}
	if !sa.DeletionTimestamp.IsZero() || bucket == "" {
		return ctrl.Result{}, r.delete(ctx, sa, bucket, folder, previousBucket, previousFolder)
	}

	if !controllerutil.ContainsFinalizer(sa, Finalizer) {
		patch := client.MergeFromWithOptions(sa.DeepCopy(), client.MergeFromWithOptimisticLock{})
		controllerutil.AddFinalizer(sa, Finalizer)
		if err := r.Client.Patch(ctx, sa, patch); err != nil {
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
	}
	location := pgbackrest.GCSFolderURI(bucket, folder)
	if sa.Annotations[ObservedFolderAnnotation] == location {
		return ctrl.Result{}, r.reconcileRestore(ctx, sa)
	}
	if err := r.createFolder(ctx, bucket, folder); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.grant(ctx, bucket, folder, Role, r.Principal(sa.Namespace, sa.Name)); err != nil {
		return ctrl.Result{}, err
	}
	if previousFolder != "" && (previousBucket != bucket || previousFolder != folder) {
		if err := r.deleteFolder(ctx, previousBucket, previousFolder); err != nil {
			return ctrl.Result{}, err
		}
	}

	patch := client.MergeFromWithOptions(sa.DeepCopy(), client.MergeFromWithOptimisticLock{})
	if sa.Annotations == nil {
		sa.Annotations = map[string]string{}
	}
	sa.Annotations[ObservedFolderAnnotation] = location
	if err := r.Client.Patch(ctx, sa, patch); err != nil {
		return ctrl.Result{}, fmt.Errorf("record observed folder: %w", err)
	}
	return ctrl.Result{}, r.reconcileRestore(ctx, sa)
}

// reconcileRestore grants RestoreRole on the requested restore folder and
// revokes it from the previous one. The folder belongs to another branch, so
// it is never created or deleted here. Callers must not change or remove the
// request until it is observed, or the unobserved grant leaks.
func (r *Reconciler) reconcileRestore(ctx context.Context, sa *corev1.ServiceAccount) error {
	requested, observed, err := restoreLocations(sa)
	if err != nil {
		return err
	}
	if requested == observed {
		return nil
	}
	member := r.Principal(sa.Namespace, sa.Name)
	if requested != "" {
		bucket, folder, _ := pgbackrest.ParseGCSFolderURI(requested)
		if err := r.grant(ctx, bucket, folder, RestoreRole, member); err != nil {
			return err
		}
	}
	if err := r.revoke(ctx, observed, member); err != nil {
		return err
	}

	patch := client.MergeFromWithOptions(sa.DeepCopy(), client.MergeFromWithOptimisticLock{})
	if requested == "" {
		delete(sa.Annotations, ObservedRestoreFolderAnnotation)
	} else {
		if sa.Annotations == nil {
			sa.Annotations = map[string]string{}
		}
		sa.Annotations[ObservedRestoreFolderAnnotation] = requested
	}
	if err := r.Client.Patch(ctx, sa, patch); err != nil {
		return fmt.Errorf("record observed restore folder: %w", err)
	}
	return nil
}

// restoreLocations returns the normalized requested and observed restore
// folder URIs.
func restoreLocations(sa *corev1.ServiceAccount) (string, string, error) {
	var locations [2]string
	for i, key := range []string{pgbackrest.GCSRestoreFolderAnnotation, ObservedRestoreFolderAnnotation} {
		if location := sa.Annotations[key]; location != "" {
			bucket, folder, err := pgbackrest.ParseGCSFolderURI(location)
			if err != nil {
				return "", "", err
			}
			locations[i] = pgbackrest.GCSFolderURI(bucket, folder)
		}
	}
	return locations[0], locations[1], nil
}

// delete removes managed folders and grants while keeping backup objects.
func (r *Reconciler) delete(ctx context.Context, sa *corev1.ServiceAccount, bucket, folder, previousBucket, previousFolder string) error {
	if !controllerutil.ContainsFinalizer(sa, Finalizer) {
		return nil
	}
	requestedRestore, observedRestore, err := restoreLocations(sa)
	if err != nil {
		return err
	}
	member := r.Principal(sa.Namespace, sa.Name)
	if err := r.revoke(ctx, observedRestore, member); err != nil {
		return err
	}
	if requestedRestore != observedRestore {
		if err := r.revoke(ctx, requestedRestore, member); err != nil {
			return err
		}
	}
	if previousFolder != "" && (previousBucket != bucket || previousFolder != folder) {
		if err := r.deleteFolder(ctx, previousBucket, previousFolder); err != nil {
			return err
		}
	}
	if folder != "" {
		if err := r.deleteFolder(ctx, bucket, folder); err != nil {
			return err
		}
	}
	patch := client.MergeFromWithOptions(sa.DeepCopy(), client.MergeFromWithOptimisticLock{})
	controllerutil.RemoveFinalizer(sa, Finalizer)
	delete(sa.Annotations, ObservedFolderAnnotation)
	delete(sa.Annotations, ObservedRestoreFolderAnnotation)
	if err := r.Client.Patch(ctx, sa, patch); err != nil {
		return fmt.Errorf("remove finalizer: %w", err)
	}
	return nil
}

func (r *Reconciler) deleteFolder(ctx context.Context, bucket, folder string) error {
	err := r.Storage.ManagedFolders.Delete(bucket, folder).AllowNonEmpty(true).Context(ctx).Do()
	if err != nil && !isStatus(err, http.StatusNotFound) {
		return fmt.Errorf("delete managed folder %s/%s: %w", bucket, folder, err)
	}
	return nil
}

func (r *Reconciler) createFolder(ctx context.Context, bucket, folder string) error {
	_, err := r.Storage.ManagedFolders.Insert(bucket, &storage.ManagedFolder{Name: folder}).Context(ctx).Do()
	if err != nil && !isStatus(err, http.StatusConflict) {
		return fmt.Errorf("create managed folder %s: %w", folder, err)
	}
	return nil
}

// grant adds member to role on folder. GCS rejects the update with 412 when
// the policy changed after it was read, and the reconciler tries again.
func (r *Reconciler) grant(ctx context.Context, bucket, folder, role, member string) error {
	policy, err := r.Storage.ManagedFolders.GetIamPolicy(bucket, folder).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("get IAM policy of managed folder %s: %w", folder, err)
	}

	var binding *storage.PolicyBindings
	for _, b := range policy.Bindings {
		if b.Role != role || b.Condition != nil {
			continue
		}
		if slices.Contains(b.Members, member) {
			return nil
		}
		binding = b
	}
	if binding != nil {
		binding.Members = append(binding.Members, member)
	} else {
		policy.Bindings = append(policy.Bindings, &storage.PolicyBindings{Role: role, Members: []string{member}})
	}

	if _, err := r.Storage.ManagedFolders.SetIamPolicy(bucket, folder, policy).Context(ctx).Do(); err != nil {
		return fmt.Errorf("set IAM policy of managed folder %s: %w", folder, err)
	}
	return nil
}

// revoke removes member from RestoreRole on the folder at location. A missing
// folder has no grant left to remove.
func (r *Reconciler) revoke(ctx context.Context, location, member string) error {
	if location == "" {
		return nil
	}
	bucket, folder, err := pgbackrest.ParseGCSFolderURI(location)
	if err != nil {
		return err
	}
	policy, err := r.Storage.ManagedFolders.GetIamPolicy(bucket, folder).Context(ctx).Do()
	if isStatus(err, http.StatusNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get IAM policy of managed folder %s: %w", folder, err)
	}

	changed := false
	policy.Bindings = slices.DeleteFunc(policy.Bindings, func(b *storage.PolicyBindings) bool {
		if b.Role != RestoreRole || b.Condition != nil || !slices.Contains(b.Members, member) {
			return false
		}
		changed = true
		b.Members = slices.DeleteFunc(b.Members, func(m string) bool { return m == member })
		return len(b.Members) == 0
	})
	if !changed {
		return nil
	}

	_, err = r.Storage.ManagedFolders.SetIamPolicy(bucket, folder, policy).Context(ctx).Do()
	if err != nil && !isStatus(err, http.StatusNotFound) {
		return fmt.Errorf("set IAM policy of managed folder %s: %w", folder, err)
	}
	return nil
}

// Principal is the IAM principal of a Kubernetes ServiceAccount in the
// Workload Identity pool.
func (r *Reconciler) Principal(namespace, serviceAccount string) string {
	return fmt.Sprintf(
		"principal://iam.googleapis.com/projects/%s/locations/global/workloadIdentityPools/%s/subject/ns/%s/sa/%s",
		r.ProjectNumber, r.WorkloadIdentityPool, namespace, serviceAccount,
	)
}

func isStatus(err error, code int) bool {
	var gerr *googleapi.Error
	return errors.As(err, &gerr) && gerr.Code == code
}
