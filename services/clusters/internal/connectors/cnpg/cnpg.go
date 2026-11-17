package cnpg

import (
	"context"
	"fmt"

	"xata/services/clusters/internal/kubernetes"

	"k8s.io/apimachinery/pkg/types"

	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

//go:generate go run github.com/vektra/mockery/v2 --with-expecter --name Connector

// Connector is an interface for interacting with the cnpg operator
type Connector interface {
	GetClusterCredentials(ctx context.Context, id, namespace, username string) (*Credentials, error)
}

type Credentials struct {
	SecretVersion string
	Username      string
	Password      string
}

type DefaultConnector struct {
	KubernetesClient client.Client
}

// NewConnector creates a client for communicating with the k8s cnpg operator
func NewConnector(kubeConfig string) (*DefaultConnector, error) {
	dcp := &kubernetes.DefaultConfigProvider{
		KubeConfigPath: kubeConfig,
		MasterURL:      "",
	}
	cfg, err := kubernetes.GetRestConfig(dcp)
	if err != nil {
		return nil, fmt.Errorf("get restconfig %w", err)
	}

	// make the client aware of the cnpg scheme
	scheme := runtime.NewScheme()
	if err = clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("add to scheme clientgo %w", err)
	}

	if err = apiv1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("add to scheme cnpg %w", err)
	}

	clientK8s, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("create clientset %w", err)
	}

	return &DefaultConnector{KubernetesClient: clientK8s}, nil
}

func (c *DefaultConnector) GetClusterCredentials(ctx context.Context, id, namespace, username string) (*Credentials, error) {
	// Read the secret with Kubernetes client
	var secret v1.Secret
	err := c.KubernetesClient.Get(ctx, types.NamespacedName{
		Namespace: namespace,
		Name:      id + "-" + username,
	}, &secret)
	if err != nil {
		return nil, err
	}

	return &Credentials{
		SecretVersion: secret.ResourceVersion,
		Username:      string(secret.Data[v1.BasicAuthUsernameKey]),
		Password:      string(secret.Data[v1.BasicAuthPasswordKey]),
	}, nil
}
