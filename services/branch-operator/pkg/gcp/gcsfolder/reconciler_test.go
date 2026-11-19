package gcsfolder_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	storage "google.golang.org/api/storage/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"xata/internal/pgbackrest"
	"xata/services/branch-operator/pkg/gcp/gcsfolder"
)

const (
	namespace = "xata-clusters"
	bucket    = "backups"
)

// fakeGCS serves the managed folder calls of the GCS JSON API.
type fakeGCS struct {
	mu          sync.Mutex
	policies    map[string]*storage.Policy // by managed folder
	calls       []string
	failRequest string
}

func (f *fakeGCS) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, req.Method)

	requestBucket, path, _ := strings.Cut(strings.TrimPrefix(req.URL.EscapedPath(), "/storage/v1/b/"), "/managedFolders")
	if f.failRequest == req.Method+" "+requestBucket {
		http.Error(w, `{"error":{"code":403}}`, http.StatusForbidden)
		return
	}
	folder, iam := strings.CutSuffix(strings.TrimPrefix(path, "/"), "/iam")
	key := requestBucket + "/" + unescape(folder)

	switch {
	case req.Method == http.MethodPost && folder == "":
		var mf storage.ManagedFolder
		_ = json.NewDecoder(req.Body).Decode(&mf)
		if _, ok := f.policies[requestBucket+"/"+mf.Name]; ok {
			http.Error(w, `{"error":{"code":409}}`, http.StatusConflict)
			return
		}
		f.policies[requestBucket+"/"+mf.Name] = &storage.Policy{Etag: "0"}
		_ = json.NewEncoder(w).Encode(mf)
	case req.Method == http.MethodGet && iam:
		policy, ok := f.policies[key]
		if !ok {
			http.Error(w, `{"error":{"code":404}}`, http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(policy)
	case req.Method == http.MethodPut && iam:
		var policy storage.Policy
		_ = json.NewDecoder(req.Body).Decode(&policy)
		f.policies[key] = &policy
		_ = json.NewEncoder(w).Encode(policy)
	case req.Method == http.MethodDelete && !iam:
		if _, ok := f.policies[key]; !ok {
			http.Error(w, `{"error":{"code":404}}`, http.StatusNotFound)
			return
		}
		delete(f.policies, key)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "unexpected request "+req.Method+" "+req.URL.String(), http.StatusBadRequest)
	}
}

func unescape(s string) string {
	return strings.NewReplacer("%2F", "/", "%3A", ":").Replace(s)
}

func setup(t *testing.T, objs ...client.Object) (*gcsfolder.Reconciler, *fakeGCS) {
	t.Helper()

	gcs := &fakeGCS{policies: map[string]*storage.Policy{}}
	srv := httptest.NewServer(gcs)
	t.Cleanup(srv.Close)

	svc, err := storage.NewService(context.Background(),
		option.WithEndpoint(srv.URL+"/storage/v1/"),
		option.WithoutAuthentication(),
		option.WithHTTPClient(srv.Client()),
	)
	require.NoError(t, err)

	return &gcsfolder.Reconciler{
		Client:               fake.NewClientBuilder().WithScheme(clientgoscheme.Scheme).WithObjects(objs...).Build(),
		Storage:              svc,
		ProjectNumber:        "123",
		WorkloadIdentityPool: "project.svc.id.goog",
	}, gcs
}

func reconcile(t *testing.T, r *gcsfolder.Reconciler, name string) {
	t.Helper()
	_, err := r.Reconcile(context.Background(), ctrl.Request{Namespace: namespace, Name: name})
	require.NoError(t, err)
}

func serviceAccount(name string) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{Namespace: namespace, Name: name}
}

func branchServiceAccount(branch string) *corev1.ServiceAccount {
	sa := serviceAccount(pgbackrest.ServiceAccountName(branch))
	sa.OwnerReferences = []metav1.OwnerReference{{Kind: "Branch", Name: branch, Controller: new(true)}}
	sa.Annotations = map[string]string{pgbackrest.GCSFolderAnnotation: "gs://" + bucket + "/" + branch + "/"}
	return sa
}

func TestReconcile(t *testing.T) {
	t.Parallel()

	folder := "b123/"
	ctx := context.Background()

	t.Run("creates folder and grant, then skips GCS", func(t *testing.T) {
		t.Parallel()
		r, gcs := setup(t, branchServiceAccount("b123"))

		reconcile(t, r, "b123-pgbackrest")

		require.Equal(t, "b123/", folder)
		require.Len(t, gcs.policies[bucket+"/"+folder].Bindings, 1)
		want := &storage.PolicyBindings{
			Role:    gcsfolder.Role,
			Members: []string{"principal://iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/project.svc.id.goog/subject/ns/xata-clusters/sa/b123-pgbackrest"},
		}
		require.Equal(t, want, gcs.policies[bucket+"/"+folder].Bindings[0])

		got := &corev1.ServiceAccount{}
		require.NoError(t, r.Client.Get(ctx, client.ObjectKeyFromObject(serviceAccount("b123-pgbackrest")), got))
		require.Contains(t, got.Finalizers, gcsfolder.Finalizer)
		require.Equal(t, "gs://"+bucket+"/"+folder, got.Annotations[gcsfolder.ObservedFolderAnnotation])
		require.NotContains(t, got.Annotations, gcsfolder.ObservedRestoreFolderAnnotation)

		calls := len(gcs.calls)
		reconcile(t, r, "b123-pgbackrest")
		require.Len(t, gcs.calls, calls)
	})

	t.Run("existing folder and grant are kept", func(t *testing.T) {
		t.Parallel()
		r, gcs := setup(t, branchServiceAccount("b123"))

		reconcile(t, r, "b123-pgbackrest")
		// Lose the annotation, as if the reconciler stopped before it wrote it.
		sa := &corev1.ServiceAccount{}
		require.NoError(t, r.Client.Get(ctx, client.ObjectKeyFromObject(serviceAccount("b123-pgbackrest")), sa))
		delete(sa.Annotations, gcsfolder.ObservedFolderAnnotation)
		require.NoError(t, r.Client.Update(ctx, sa))

		reconcile(t, r, "b123-pgbackrest")
		require.Len(t, gcs.policies[bucket+"/"+folder].Bindings, 1)
	})

	t.Run("deletes folder before the ServiceAccount", func(t *testing.T) {
		t.Parallel()
		r, gcs := setup(t, branchServiceAccount("b123"))

		reconcile(t, r, "b123-pgbackrest")
		require.NoError(t, r.Client.Delete(ctx, serviceAccount("b123-pgbackrest")))

		reconcile(t, r, "b123-pgbackrest")
		require.NotContains(t, gcs.policies, bucket+"/"+folder)
		err := r.Client.Get(ctx, client.ObjectKeyFromObject(serviceAccount("b123-pgbackrest")), &corev1.ServiceAccount{})
		require.True(t, apierrors.IsNotFound(err))
	})

	t.Run("ignores other ServiceAccounts", func(t *testing.T) {
		t.Parallel()
		branchID := serviceAccount("b456-pgbackrest")
		branchID.Labels = map[string]string{"xata.io/branchID": "b456"}
		noBucket := branchServiceAccount("b789")
		noBucket.Annotations = nil
		r, gcs := setup(t, serviceAccount("b123"), branchID, noBucket)

		reconcile(t, r, "b123")
		reconcile(t, r, "b456-pgbackrest")
		reconcile(t, r, "b789-pgbackrest")
		require.Empty(t, gcs.calls)
	})
}

func TestReconcileGrantKeepsOtherBindings(t *testing.T) {
	t.Parallel()
	r, gcs := setup(t, branchServiceAccount("b123"))
	folder := "b123/"

	other := &storage.PolicyBindings{Role: "roles/storage.objectViewer", Members: []string{"principal://restore"}}
	gcs.policies[bucket+"/"+folder] = &storage.Policy{Etag: "0", Bindings: []*storage.PolicyBindings{other}}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := r.Reconcile(ctx, ctrl.Request{Namespace: namespace, Name: "b123-pgbackrest"})
	require.NoError(t, err)

	require.Len(t, gcs.policies[bucket+"/"+folder].Bindings, 2)
	require.Equal(t, other, gcs.policies[bucket+"/"+folder].Bindings[0])
}

func TestReconcileBucketChange(t *testing.T) {
	t.Parallel()
	for name, deletion := range map[string]bool{"moves grant": false, "deletes before move": true} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			r, gcs := setup(t, branchServiceAccount("b123"))
			reconcile(t, r, "b123-pgbackrest")
			sa := serviceAccount("b123-pgbackrest")
			require.NoError(t, r.Client.Get(t.Context(), client.ObjectKeyFromObject(sa), sa))
			sa.Annotations[pgbackrest.GCSFolderAnnotation] = "gs://new-backups/b123/"
			require.NoError(t, r.Client.Update(t.Context(), sa))
			if deletion {
				require.NoError(t, r.Client.Delete(t.Context(), sa))
			}
			reconcile(t, r, sa.Name)
			require.NotContains(t, gcs.policies, bucket+"/b123/")
			if deletion {
				require.Empty(t, gcs.policies)
				require.True(t, apierrors.IsNotFound(r.Client.Get(t.Context(), client.ObjectKeyFromObject(sa), sa)))
				return
			}
			require.Len(t, gcs.policies["new-backups/b123/"].Bindings, 1)
			require.NoError(t, r.Client.Get(t.Context(), client.ObjectKeyFromObject(sa), sa))
			require.Equal(t, "gs://new-backups/b123/", sa.Annotations[gcsfolder.ObservedFolderAnnotation])
			require.NoError(t, r.Client.Delete(t.Context(), sa))
			reconcile(t, r, sa.Name)
			require.Empty(t, gcs.policies)
		})
	}
}

func TestReconcileBucketChangeRetry(t *testing.T) {
	t.Parallel()
	for name, failedRequest := range map[string]string{"old folder deletion": "DELETE " + bucket, "new folder creation": "POST new-backups", "new folder grant": "PUT new-backups"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			r, gcs := setup(t, branchServiceAccount("b123"))
			reconcile(t, r, "b123-pgbackrest")
			sa := serviceAccount("b123-pgbackrest")
			require.NoError(t, r.Client.Get(t.Context(), client.ObjectKeyFromObject(sa), sa))
			sa.Annotations[pgbackrest.GCSFolderAnnotation] = "gs://new-backups/b123/"
			require.NoError(t, r.Client.Update(t.Context(), sa))
			gcs.failRequest = failedRequest
			_, err := r.Reconcile(t.Context(), ctrl.Request{Namespace: namespace, Name: sa.Name})
			require.Error(t, err)
			require.NoError(t, r.Client.Get(t.Context(), client.ObjectKeyFromObject(sa), sa))
			want := "gs://" + bucket + "/b123/"
			require.Equal(t, want, sa.Annotations[gcsfolder.ObservedFolderAnnotation])
			require.Contains(t, gcs.policies, bucket+"/b123/")
			gcs.failRequest = ""
			reconcile(t, r, sa.Name)
			require.NotContains(t, gcs.policies, bucket+"/b123/")
			require.Len(t, gcs.policies["new-backups/b123/"].Bindings, 1)
		})
	}
}

func TestReconcileFolderURI(t *testing.T) {
	t.Parallel()
	sa := branchServiceAccount("b123")
	sa.Annotations[pgbackrest.GCSFolderAnnotation] = "gs://other-backups/team/repository/"
	r, gcs := setup(t, sa)
	reconcile(t, r, sa.Name)
	require.Len(t, gcs.policies["other-backups/team/repository/"].Bindings, 1)
	require.NoError(t, r.Client.Delete(t.Context(), sa))
	reconcile(t, r, sa.Name)
	require.Empty(t, gcs.policies)
}

func TestReconcileRepositoryLocationChange(t *testing.T) {
	t.Parallel()
	for name, target := range map[string]string{
		"bucket":          "gs://new-backups/b123/",
		"repository path": "gs://backups/team/repository/",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			sa := branchServiceAccount("b123")
			sa.Annotations[pgbackrest.GCSFolderAnnotation] = "gs://backups/b123/"
			r, gcs := setup(t, sa)
			reconcile(t, r, sa.Name)
			require.NoError(t, r.Client.Get(t.Context(), client.ObjectKeyFromObject(sa), sa))
			sa.Annotations[pgbackrest.GCSFolderAnnotation] = target
			require.NoError(t, r.Client.Update(t.Context(), sa))
			reconcile(t, r, sa.Name)
			require.NotContains(t, gcs.policies, "backups/b123/")
			require.Len(t, gcs.policies[strings.TrimPrefix(target, "gs://")].Bindings, 1)
			require.NoError(t, r.Client.Get(t.Context(), client.ObjectKeyFromObject(sa), sa))
			require.Equal(t, target, sa.Annotations[gcsfolder.ObservedFolderAnnotation])
			require.NoError(t, r.Client.Delete(t.Context(), sa))
			reconcile(t, r, sa.Name)
			require.Empty(t, gcs.policies)
		})
	}
}

func TestReconcileObservedWriteRetry(t *testing.T) {
	t.Parallel()
	for name, deleteAccount := range map[string]bool{"retry": false, "delete before retry": true} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			sa := branchServiceAccount("b123")
			r, gcs := setup(t, sa)
			reconcile(t, r, sa.Name)
			require.NoError(t, r.Client.Get(t.Context(), client.ObjectKeyFromObject(sa), sa))
			sa.Annotations[pgbackrest.GCSFolderAnnotation] = "gs://new-backups/b123/"
			require.NoError(t, r.Client.Update(t.Context(), sa))
			watchClient, ok := r.Client.(client.WithWatch)
			require.True(t, ok)
			r.Client = interceptor.NewClient(watchClient, interceptor.Funcs{
				Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					if obj.GetAnnotations()[gcsfolder.ObservedFolderAnnotation] == "gs://new-backups/b123/" {
						return apierrors.NewConflict(schema.GroupResource{Resource: "serviceaccounts"}, obj.GetName(), nil)
					}
					return c.Patch(ctx, obj, patch, opts...)
				},
			})
			_, err := r.Reconcile(t.Context(), ctrl.Request{Namespace: namespace, Name: sa.Name})
			require.Error(t, err)
			require.Len(t, gcs.policies["new-backups/b123/"].Bindings, 1)
			require.NoError(t, r.Client.Get(t.Context(), client.ObjectKeyFromObject(sa), sa))
			require.Equal(t, "gs://backups/b123/", sa.Annotations[gcsfolder.ObservedFolderAnnotation])
			r.Client = watchClient
			if deleteAccount {
				require.NoError(t, r.Client.Delete(t.Context(), sa))
			}
			reconcile(t, r, sa.Name)
			if deleteAccount {
				require.Empty(t, gcs.policies)
			} else {
				require.Len(t, gcs.policies["new-backups/b123/"].Bindings, 1)
				require.NoError(t, r.Client.Get(t.Context(), client.ObjectKeyFromObject(sa), sa))
				require.Equal(t, "gs://new-backups/b123/", sa.Annotations[gcsfolder.ObservedFolderAnnotation])
			}
		})
	}
}

func TestReconcileRemovedFolderRequest(t *testing.T) {
	t.Parallel()
	sa := branchServiceAccount("b123")
	sa.Annotations[pgbackrest.GCSFolderAnnotation] = "gs://backups/b123/"
	r, gcs := setup(t, sa)
	reconcile(t, r, sa.Name)
	require.NoError(t, r.Client.Get(t.Context(), client.ObjectKeyFromObject(sa), sa))
	delete(sa.Annotations, pgbackrest.GCSFolderAnnotation)
	require.NoError(t, r.Client.Update(t.Context(), sa))
	reconcile(t, r, sa.Name)
	require.Empty(t, gcs.policies)
	require.NoError(t, r.Client.Get(t.Context(), client.ObjectKeyFromObject(sa), sa))
	require.NotContains(t, sa.Finalizers, gcsfolder.Finalizer)
	require.NotContains(t, sa.Annotations, gcsfolder.ObservedFolderAnnotation)
}

const (
	sourceFolder = bucket + "/a123/"
	otherViewer  = "principal://other-restore"
)

// restoreSetup returns a reconciled b123 ServiceAccount that restores from
// the folder of a123, which another branch also restores from.
func restoreSetup(t *testing.T) (*gcsfolder.Reconciler, *fakeGCS, *corev1.ServiceAccount) {
	t.Helper()
	sa := branchServiceAccount("b123")
	sa.Annotations[pgbackrest.GCSRestoreFolderAnnotation] = "gs://" + sourceFolder
	r, gcs := setup(t, sa)
	gcs.policies[sourceFolder] = &storage.Policy{Etag: "0", Bindings: []*storage.PolicyBindings{
		{Role: gcsfolder.Role, Members: []string{r.Principal(namespace, "a123-pgbackrest")}},
		{Role: gcsfolder.RestoreRole, Members: []string{otherViewer}},
	}}
	reconcile(t, r, sa.Name)
	require.NoError(t, r.Client.Get(t.Context(), client.ObjectKeyFromObject(sa), sa))
	return r, gcs, sa
}

func requireSourceGrants(t *testing.T, r *gcsfolder.Reconciler, gcs *fakeGCS, viewers ...string) {
	t.Helper()
	want := []*storage.PolicyBindings{
		{Role: gcsfolder.Role, Members: []string{r.Principal(namespace, "a123-pgbackrest")}},
		{Role: gcsfolder.RestoreRole, Members: viewers},
	}
	require.Equal(t, want, gcs.policies[sourceFolder].Bindings)
}

func TestReconcileRestoreFolder(t *testing.T) {
	t.Parallel()
	r, gcs, sa := restoreSetup(t)
	member := r.Principal(namespace, sa.Name)

	requireSourceGrants(t, r, gcs, otherViewer, member)
	require.Len(t, gcs.policies[bucket+"/b123/"].Bindings, 1)
	require.Equal(t, "gs://"+sourceFolder, sa.Annotations[gcsfolder.ObservedRestoreFolderAnnotation])

	calls := len(gcs.calls)
	reconcile(t, r, sa.Name)
	require.Len(t, gcs.calls, calls)

	delete(sa.Annotations, pgbackrest.GCSRestoreFolderAnnotation)
	require.NoError(t, r.Client.Update(t.Context(), sa))
	reconcile(t, r, sa.Name)
	requireSourceGrants(t, r, gcs, otherViewer)
	require.Len(t, gcs.policies[bucket+"/b123/"].Bindings, 1)
	require.NoError(t, r.Client.Get(t.Context(), client.ObjectKeyFromObject(sa), sa))
	require.NotContains(t, sa.Annotations, gcsfolder.ObservedRestoreFolderAnnotation)
	require.Contains(t, sa.Finalizers, gcsfolder.Finalizer)
}

func TestReconcileRestoreFolderChange(t *testing.T) {
	t.Parallel()
	r, gcs, sa := restoreSetup(t)
	member := r.Principal(namespace, sa.Name)
	gcs.policies[bucket+"/c123/"] = &storage.Policy{Etag: "0"}

	sa.Annotations[pgbackrest.GCSRestoreFolderAnnotation] = "gs://" + bucket + "/c123/"
	require.NoError(t, r.Client.Update(t.Context(), sa))
	reconcile(t, r, sa.Name)
	requireSourceGrants(t, r, gcs, otherViewer)
	require.Equal(t, []*storage.PolicyBindings{{Role: gcsfolder.RestoreRole, Members: []string{member}}}, gcs.policies[bucket+"/c123/"].Bindings)
	require.NoError(t, r.Client.Get(t.Context(), client.ObjectKeyFromObject(sa), sa))
	require.Equal(t, "gs://"+bucket+"/c123/", sa.Annotations[gcsfolder.ObservedRestoreFolderAnnotation])
}

func TestReconcileRestoreFolderDeletion(t *testing.T) {
	t.Parallel()
	r, gcs, sa := restoreSetup(t)

	require.NoError(t, r.Client.Delete(t.Context(), sa))
	reconcile(t, r, sa.Name)
	requireSourceGrants(t, r, gcs, otherViewer)
	require.NotContains(t, gcs.policies, bucket+"/b123/")
	require.True(t, apierrors.IsNotFound(r.Client.Get(t.Context(), client.ObjectKeyFromObject(sa), sa)))
}

func TestReconcileRestoreFolderSourceDeleted(t *testing.T) {
	t.Parallel()
	for name, deleteAccount := range map[string]bool{"annotation removed": false, "ServiceAccount deleted": true} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			r, gcs, sa := restoreSetup(t)
			delete(gcs.policies, sourceFolder)

			if deleteAccount {
				require.NoError(t, r.Client.Delete(t.Context(), sa))
				reconcile(t, r, sa.Name)
				require.True(t, apierrors.IsNotFound(r.Client.Get(t.Context(), client.ObjectKeyFromObject(sa), sa)))
				require.Empty(t, gcs.policies)
				return
			}
			delete(sa.Annotations, pgbackrest.GCSRestoreFolderAnnotation)
			require.NoError(t, r.Client.Update(t.Context(), sa))
			reconcile(t, r, sa.Name)
			require.NoError(t, r.Client.Get(t.Context(), client.ObjectKeyFromObject(sa), sa))
			require.NotContains(t, sa.Annotations, gcsfolder.ObservedRestoreFolderAnnotation)
		})
	}
}

func TestReconcileRestoreFolderMissing(t *testing.T) {
	t.Parallel()
	sa := branchServiceAccount("b123")
	sa.Annotations[pgbackrest.GCSRestoreFolderAnnotation] = "gs://" + sourceFolder
	r, gcs := setup(t, sa)

	_, err := r.Reconcile(t.Context(), ctrl.Request{Namespace: namespace, Name: sa.Name})
	require.Error(t, err)
	require.NotContains(t, gcs.policies, sourceFolder)
	require.NoError(t, r.Client.Get(t.Context(), client.ObjectKeyFromObject(sa), sa))
	require.NotContains(t, sa.Annotations, gcsfolder.ObservedRestoreFolderAnnotation)
	require.Equal(t, "gs://"+bucket+"/b123/", sa.Annotations[gcsfolder.ObservedFolderAnnotation])

	gcs.policies[sourceFolder] = &storage.Policy{Etag: "0"}
	reconcile(t, r, sa.Name)
	require.NoError(t, r.Client.Get(t.Context(), client.ObjectKeyFromObject(sa), sa))
	require.Equal(t, "gs://"+sourceFolder, sa.Annotations[gcsfolder.ObservedRestoreFolderAnnotation])
}

func TestReconcileRestoreObservedWriteRetry(t *testing.T) {
	t.Parallel()
	for name, deleteAccount := range map[string]bool{"retry": false, "delete before retry": true} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			sa := branchServiceAccount("b123")
			r, gcs := setup(t, sa)
			reconcile(t, r, sa.Name)
			gcs.policies[sourceFolder] = &storage.Policy{Etag: "0", Bindings: []*storage.PolicyBindings{
				{Role: gcsfolder.Role, Members: []string{r.Principal(namespace, "a123-pgbackrest")}},
				{Role: gcsfolder.RestoreRole, Members: []string{otherViewer}},
			}}
			require.NoError(t, r.Client.Get(t.Context(), client.ObjectKeyFromObject(sa), sa))
			sa.Annotations[pgbackrest.GCSRestoreFolderAnnotation] = "gs://" + sourceFolder
			require.NoError(t, r.Client.Update(t.Context(), sa))
			watchClient, ok := r.Client.(client.WithWatch)
			require.True(t, ok)
			r.Client = interceptor.NewClient(watchClient, interceptor.Funcs{
				Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					if obj.GetAnnotations()[gcsfolder.ObservedRestoreFolderAnnotation] != "" {
						return apierrors.NewConflict(schema.GroupResource{Resource: "serviceaccounts"}, obj.GetName(), nil)
					}
					return c.Patch(ctx, obj, patch, opts...)
				},
			})
			_, err := r.Reconcile(t.Context(), ctrl.Request{Namespace: namespace, Name: sa.Name})
			require.Error(t, err)
			requireSourceGrants(t, r, gcs, otherViewer, r.Principal(namespace, sa.Name))
			require.NoError(t, r.Client.Get(t.Context(), client.ObjectKeyFromObject(sa), sa))
			require.NotContains(t, sa.Annotations, gcsfolder.ObservedRestoreFolderAnnotation)
			r.Client = watchClient
			if deleteAccount {
				require.NoError(t, r.Client.Delete(t.Context(), sa))
			}
			reconcile(t, r, sa.Name)
			if deleteAccount {
				requireSourceGrants(t, r, gcs, otherViewer)
				require.True(t, apierrors.IsNotFound(r.Client.Get(t.Context(), client.ObjectKeyFromObject(sa), sa)))
				return
			}
			requireSourceGrants(t, r, gcs, otherViewer, r.Principal(namespace, sa.Name))
			require.NoError(t, r.Client.Get(t.Context(), client.ObjectKeyFromObject(sa), sa))
			require.Equal(t, "gs://"+sourceFolder, sa.Annotations[gcsfolder.ObservedRestoreFolderAnnotation])
		})
	}
}
