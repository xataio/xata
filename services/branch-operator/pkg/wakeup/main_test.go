package wakeup_test

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"xata/internal/envtestutil"
	poolv1alpha1 "xata/proto/clusterpool-operator/api/v1alpha1"
	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/shared"
	"xata/services/branch-operator/pkg/wakeup"

	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	slotv1 "xata/gen/proto/slots/v1"
)

const (
	BranchOperatorCRDsPath   = "../../../../charts/branch-operator/crds/"
	ClusterPoolCRDsPath      = "../../../../saas-charts/clusterpool-operator/crds/"
	SharedThirdPartyCRDsPath = "../../testutils/crds/"
	TestNamespace            = "xata-wakeup-test"
)

// k8sClient is a live Kubernetes client connected to the test environment.
// It does not use the controller cache for either reads or writes.
var (
	k8sClient  client.Client
	testScheme *runtime.Scheme
)

// Make envtestutil functions available in this package for convenience
var (
	randomString          = envtestutil.RandomString
	requireEventuallyTrue = envtestutil.RequireEventuallyTrue
)

// testSlotController is a minimal SlotController gRPC server that returns
// success for all WakeUp calls.
type testSlotController struct {
	slotv1.UnimplementedSlotControllerServer
}

func (t *testSlotController) WakeUp(_ context.Context, _ *slotv1.WakeUpRequest) (*slotv1.WakeUpResponse, error) {
	return &slotv1.WakeUpResponse{}, nil
}

// TestMain sets up the envtest integration test environment using the
// `envtestutil` package and runs the tests.
func TestMain(m *testing.M) {
	// Crete a test gRPC server for the SlotController service
	grpcServer := grpc.NewServer()
	slotv1.RegisterSlotControllerServer(grpcServer, &testSlotController{})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}

	// Start the SlotController gRPC server in a separate goroutine
	go grpcServer.Serve(lis)
	csiNodePort := lis.Addr().(*net.TCPAddr).Port //nolint:forcetypeassert

	env := envtestutil.Setup(
		envtestutil.Options{
			CRDDirectoryPaths: []string{
				BranchOperatorCRDsPath,
				ClusterPoolCRDsPath,
				SharedThirdPartyCRDsPath,
			},
			SchemeAdders: []func(*runtime.Scheme) error{
				corev1.AddToScheme,
				eventsv1.AddToScheme,
				v1alpha1.AddToScheme,
				poolv1alpha1.AddToScheme,
				apiv1.AddToScheme,
			},
			Namespaces: []string{TestNamespace},
			ReconcilerSetup: func(ctx context.Context, mgr ctrl.Manager) error {
				r := &wakeup.WakeupReconciler{
					Client:              mgr.GetClient(),
					Scheme:              mgr.GetScheme(),
					Recorder:            mgr.GetEventRecorder(wakeup.ReconcilerName),
					CSINodeNamespace:    TestNamespace,
					WakeupRequestTTL:    1 * time.Second,
					WakeupRPCTimeout:    10 * time.Second,
					PasswordSyncTimeout: 1 * time.Second,
					CSINodePort:         csiNodePort,
				}
				return r.SetupWithManager(ctx, mgr)
			},
		})
	k8sClient = env.Client
	testScheme = env.Manager.GetScheme()

	// Run the tests
	code := m.Run()

	grpcServer.GracefulStop()
	env.Teardown()

	// Exit with the test return code
	os.Exit(code)
}

// setupPoolCluster creates a CNPG Cluster owned by the given ClusterPool along
// with all dependent objects needed for the wakeup reconciler: a PV with a CSI
// volume handle, a PVC bound to it, a primary pod scheduled to a node, and a
// CSI node plugin pod on the same node.
func setupPoolCluster(ctx context.Context, pool *poolv1alpha1.ClusterPool, name, namespace string, readyInstances int) (*apiv1.Cluster, error) {
	cluster := &apiv1.Cluster{
		Name:      name,
		Namespace: namespace,
		Spec: apiv1.ClusterSpec{
			Instances: 1,
		},
	}

	// Set the Cluster's controller owner reference to the ClusterPool so that it
	// is considered part of the pool
	if err := controllerutil.SetControllerReference(pool, cluster, testScheme); err != nil {
		return nil, err
	}

	// Create the Cluster
	if err := k8sClient.Create(ctx, cluster); err != nil {
		return nil, err
	}

	// Create a PV with a CSI volume handle for the Cluster
	pv := &corev1.PersistentVolume{
		Name: name + "-pv",
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       "test.csi.driver",
					VolumeHandle: "slot/" + name,
				},
			},
		},
	}
	if err := k8sClient.Create(ctx, pv); err != nil {
		return nil, err
	}

	// Create a PVC bound to the PV
	pvc := &corev1.PersistentVolumeClaim{
		Name:      name + "-1",
		Namespace: namespace,
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
			VolumeName: name + "-pv",
		},
	}
	if err := k8sClient.Create(ctx, pvc); err != nil {
		return nil, err
	}

	// Create the primary pod on a unique test node
	nodeName := "test-node-" + name
	primaryPod := &corev1.Pod{
		Name:      name + "-1",
		Namespace: namespace,
		Spec: corev1.PodSpec{
			NodeName:   nodeName,
			Containers: []corev1.Container{{Name: "postgres", Image: "postgres:17"}},
		},
	}
	if err := k8sClient.Create(ctx, primaryPod); err != nil {
		return nil, err
	}

	// Create a CSI node plugin pod on the same node
	csiNodePod := &corev1.Pod{
		Name:      "xatastor-csi-node-" + name,
		Namespace: TestNamespace,
		Labels: map[string]string{
			"app.kubernetes.io/component": "csi-node",
			"app.kubernetes.io/name":      "xatastor-csi",
		},
		Spec: corev1.PodSpec{
			NodeName:   nodeName,
			Containers: []corev1.Container{{Name: "csi-node", Image: "xatastor-csi-node:latest"}},
		},
	}
	if err := k8sClient.Create(ctx, csiNodePod); err != nil {
		return nil, err
	}

	// Set PodIP on the CSI node pod so the reconciler can dial the test gRPC
	// server
	csiNodePod.Status.PodIP = "127.0.0.1"
	if err := k8sClient.Status().Update(ctx, csiNodePod); err != nil {
		return nil, err
	}

	// Set the status phase and current primary PVC via a status subresource update
	cluster.Status.TargetPrimary = name + "-1"
	cluster.Status.ReadyInstances = readyInstances
	cluster.Status.HealthyPVC = []string{name + "-1"}
	if err := k8sClient.Status().Update(ctx, cluster); err != nil {
		return nil, err
	}

	return cluster, nil
}

// createAppSecret creates the "<branch>-app" secret holding the xata role
// credentials for the branch. In production the clusters service creates it
// before the Branch; it does not run in this test environment.
func createAppSecret(ctx context.Context, branch *v1alpha1.Branch, namespace string) (*corev1.Secret, error) {
	secret := &corev1.Secret{
		Name:      branch.Name + "-app",
		Namespace: namespace,
		Type:      corev1.SecretTypeBasicAuth,
		Data: map[string][]byte{
			corev1.BasicAuthUsernameKey: []byte("xata"),
			corev1.BasicAuthPasswordKey: []byte("password"),
		},
	}
	if err := k8sClient.Create(ctx, secret); err != nil {
		return nil, err
	}
	return secret, nil
}

// seedPasswordSync simulates the state that the wakeup reconciler waits for in
// waitForRolePasswordSync. It creates the app secret and stamps the Cluster's
// password status for the "xata" role with the secret's resource version (as
// CNPG would once it applies the role password; CNPG does not run in this test
// environment), so that the wait succeeds on its first poll.
func seedPasswordSync(ctx context.Context, branch *v1alpha1.Branch, cluster *apiv1.Cluster) error {
	secret, err := createAppSecret(ctx, branch, cluster.Namespace)
	if err != nil {
		return err
	}

	cluster.Status.ManagedRolesStatus.PasswordStatus = map[string]apiv1.PasswordState{
		"xata": {SecretResourceVersion: secret.ResourceVersion},
	}

	return k8sClient.Status().Update(ctx, cluster)
}

// clusterHasXataRole reports whether the Cluster's spec has the 'xata' managed
// role configured
func clusterHasXataRole(cluster *apiv1.Cluster) bool {
	if cluster.Spec.Managed == nil {
		return false
	}
	for _, role := range cluster.Spec.Managed.Roles {
		if role.Name == shared.LegacySQLIdentity().Login {
			return true
		}
	}
	return false
}

// createBranch creates a Branch with the given name, annotations, and the
// default test spec. It also creates an XVol named "xvol-<name>" in the
// Available state.
func createBranch(ctx context.Context, name string, annotations map[string]string) (*v1alpha1.Branch, error) {
	branch := &v1alpha1.Branch{
		Name:        name,
		Annotations: annotations,
		Spec: v1alpha1.BranchSpec{
			ClusterSpec: v1alpha1.ClusterSpec{
				Instances: 1,
				Storage:   v1alpha1.StorageSpec{Size: "1Gi"},
				Image:     "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.7",
			},
		},
	}

	if err := k8sClient.Create(ctx, branch); err != nil {
		return nil, err
	}

	// Create an XVol in the Available state for the branch
	if err := createXVol(ctx, "xvol-"+name, "Available"); err != nil {
		return nil, err
	}

	return branch, nil
}

// createXVol creates a cluster-scoped XVol resource with the given name and
// status.volumeState.
func createXVol(ctx context.Context, name, state string) error {
	gvk := schema.GroupVersionKind{Group: "xata.io", Version: "v1alpha1", Kind: "Xvol"}

	xvol := &unstructured.Unstructured{}
	xvol.SetGroupVersionKind(gvk)
	xvol.SetName(name)
	if err := unstructured.SetNestedField(xvol.Object, "1Gi", "spec", "size"); err != nil {
		return err
	}
	if err := k8sClient.Create(ctx, xvol); err != nil {
		return err
	}

	if err := unstructured.SetNestedField(xvol.Object, state, "status", "volumeState"); err != nil {
		return err
	}
	return k8sClient.Status().Update(ctx, xvol)
}

// setXVolState updates an existing XVol's status.volumeState
func setXVolState(ctx context.Context, name, state string) error {
	gvk := schema.GroupVersionKind{Group: "xata.io", Version: "v1alpha1", Kind: "Xvol"}

	xvol := &unstructured.Unstructured{}
	xvol.SetGroupVersionKind(gvk)
	if err := k8sClient.Get(ctx, client.ObjectKey{Name: name}, xvol); err != nil {
		return err
	}

	if err := unstructured.SetNestedField(xvol.Object, state, "status", "volumeState"); err != nil {
		return err
	}
	return k8sClient.Status().Update(ctx, xvol)
}

// deleteXVol deletes an XVol by name
func deleteXVol(ctx context.Context, name string) error {
	gvk := schema.GroupVersionKind{Group: "xata.io", Version: "v1alpha1", Kind: "Xvol"}

	xvol := &unstructured.Unstructured{}
	xvol.SetGroupVersionKind(gvk)
	xvol.SetName(name)
	return k8sClient.Delete(ctx, xvol)
}

// createWakeupRequest creates a WakeupRequest in the test namespace for the
// given branch name.
func createWakeupRequest(ctx context.Context, name, branchName string) (*v1alpha1.WakeupRequest, error) {
	return createWakeupRequestWithPasswordSync(ctx, name, branchName, v1alpha1.PasswordSyncModeWait)
}

// createWakeupRequestWithPasswordSync is like createWakeupRequest but sets the
// WakeupRequest's PasswordSync mode explicitly.
func createWakeupRequestWithPasswordSync(ctx context.Context, name, branchName string, mode v1alpha1.PasswordSyncMode) (*v1alpha1.WakeupRequest, error) {
	wr := &v1alpha1.WakeupRequest{
		Name:      name,
		Namespace: TestNamespace,
		Spec: v1alpha1.WakeupRequestSpec{
			BranchName:   branchName,
			XVolName:     "xvol-" + branchName,
			PasswordSync: mode,
		},
	}

	if err := k8sClient.Create(ctx, wr); err != nil {
		return nil, err
	}

	return wr, nil
}

// requireWakeupSucceededCondition polls the WakeupRequest until its Succeeded
// condition matches the expected status and reason.
func requireWakeupSucceededCondition(t *testing.T, ctx context.Context, wr *v1alpha1.WakeupRequest, status metav1.ConditionStatus, reason string) {
	t.Helper()

	requireEventuallyTrue(t, func() bool {
		w := &v1alpha1.WakeupRequest{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(wr), w); err != nil {
			return false
		}

		cond := meta.FindStatusCondition(w.Status.Conditions, v1alpha1.WakeupSucceededConditionType)
		if cond == nil {
			return false
		}

		return cond.Status == status && cond.Reason == reason
	})
}

// requireEventEventually polls until an Event with the given type and reason
// has been recorded regarding the WakeupRequest.
func requireEventEventually(t *testing.T, ctx context.Context, wr *v1alpha1.WakeupRequest, eventType, reason string) {
	t.Helper()

	requireEventuallyTrue(t, func() bool {
		eventList := &eventsv1.EventList{}
		if err := k8sClient.List(ctx, eventList, client.InNamespace(wr.Namespace)); err != nil {
			return false
		}

		for _, e := range eventList.Items {
			if e.Type == eventType && e.Reason == reason && e.Regarding.UID == wr.UID {
				return true
			}
		}

		return false
	})
}
