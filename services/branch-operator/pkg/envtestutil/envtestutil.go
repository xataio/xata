package envtestutil

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"syscall"

	"xata/internal/o11y"

	"github.com/go-logr/zerologr"
	"github.com/rs/zerolog"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// Options configures the envtest environment.
type Options struct {
	// CRDDirectoryPaths is a list of paths to directories containing CRD YAML
	// files.
	CRDDirectoryPaths []string

	// SchemeAdders is a list of functions that register API types with the
	// runtime scheme.
	SchemeAdders []func(*runtime.Scheme) error

	// Namespaces is a list of namespaces to create before tests run.
	Namespaces []string

	// ReconcilerSetup is called to wire up the reconciler(s) with the controller
	// manager.
	ReconcilerSetup func(ctx context.Context, mgr ctrl.Manager) error
}

// Env holds the running envtest environment and a live Kubernetes client.
type Env struct {
	// Client is a live Kubernetes client connected to the test environment. It
	// does not use the controller cache for either reads or writes.
	Client client.Client

	// Manager is the controller manager running the reconciler(s).
	Manager ctrl.Manager

	// cancel stops the controller manager's context.
	cancel context.CancelFunc

	// testEnv is the underlying envtest environment to stop on teardown.
	testEnv *envtest.Environment
}

// Setup creates and starts an envtest environment, returning an Env that
// callers use for assertions. Call Teardown after m.Run() completes.
func Setup(opts Options) *Env {
	ctx := context.Background()

	// Setup envtest binary assets directory
	path, err := envtest.SetupEnvtestDefaultBinaryAssetsDirectory()
	if err != nil {
		log.Fatalf("set up envtest binary assets directory: %v", err)
	}

	// Configure the scheme
	scheme := runtime.NewScheme()
	for _, add := range opts.SchemeAdders {
		if err := add(scheme); err != nil {
			log.Fatalf("add to scheme: %v", err)
		}
	}

	// Create the test environment
	testEnv := &envtest.Environment{
		CRDDirectoryPaths:           opts.CRDDirectoryPaths,
		ErrorIfCRDPathMissing:       true,
		DownloadBinaryAssets:        true,
		DownloadBinaryAssetsVersion: "v1.33.0",
		BinaryAssetsDirectory:       path,
		Scheme:                      scheme,
	}

	// Configure the API server with larger service cluster IP range to avoid
	// exhaustion during parallel test execution.
	testEnv.ControlPlane.GetAPIServer().
		Configure().Set("service-cluster-ip-range", "10.96.0.0/12")

	// Start the test environment
	unlock := acquireEnvtestAssetsLock(path)
	cfg, err := testEnv.Start()
	unlock()
	if err != nil {
		log.Fatalf("start test environment: %v", err)
	}

	// Create a controller manager
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		log.Fatalf("create controller manager: %v", err)
	}

	// Set up controller-runtime logger
	logger := o11y.NewLogger(os.Stderr, &o11y.Config{LogLevel: zerolog.TraceLevel})
	ctrlLogger := logger.With().Str("module", "controller-runtime").Logger()
	ctrl.SetLogger(zerologr.New(&ctrlLogger))

	// Create and set up the reconciler
	if opts.ReconcilerSetup != nil {
		if err := opts.ReconcilerSetup(ctx, mgr); err != nil {
			log.Fatalf("set up reconciler: %v", err)
		}
	}

	// Create a live k8s client for test assertions
	k8sClient, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		log.Fatalf("create k8s client: %v", err)
	}

	// Create the requested namespaces
	for _, ns := range opts.Namespaces {
		err = k8sClient.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: ns},
		})
		if err != nil {
			log.Fatalf("create namespace %q: %v", ns, err)
		}
	}

	// Start the manager
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		if err := mgr.Start(ctx); err != nil {
			log.Fatalf("start manager: %v", err)
		}
	}()

	// Wait for the manager's cache to sync
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		log.Fatal("sync cache")
	}

	return &Env{
		Client:  k8sClient,
		Manager: mgr,
		cancel:  cancel,
		testEnv: testEnv,
	}
}

func acquireEnvtestAssetsLock(binaryAssetsDirectory string) func() {
	if err := os.MkdirAll(binaryAssetsDirectory, 0o700); err != nil {
		log.Fatalf("create envtest assets directory: %v", err)
	}

	lockFile, err := os.OpenFile(filepath.Join(binaryAssetsDirectory, ".envtest.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		log.Fatalf("open envtest assets lock: %v", err)
	}

	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX); err != nil {
		_ = lockFile.Close()
		log.Fatalf("lock envtest assets: %v", err)
	}

	return func() {
		if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN); err != nil {
			log.Fatalf("unlock envtest assets: %v", err)
		}
		if err := lockFile.Close(); err != nil {
			log.Fatalf("close envtest assets lock: %v", err)
		}
	}
}

// Teardown stops the manager and the envtest environment.
func (e *Env) Teardown() {
	e.cancel()
	if err := e.testEnv.Stop(); err != nil {
		log.Fatalf("stop test environment: %v", err)
	}
}
