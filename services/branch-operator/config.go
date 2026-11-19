package branchoperator

import (
	"time"

	"xata/internal/envcfg"
)

type Config struct {
	ClustersNamespace              string `env:"XATA_CLUSTERS_NAMESPACE" env-default:"xata-clusters" env-description:"namespace where the operator creates managed resources"`
	BackupsBucket                  string `env:"XATA_BACKUPS_BUCKET" env-description:"bucket for storing the cluster backups"`
	BackupsEndpoint                string `env:"XATA_BACKUPS_ENDPOINT" env-description:"endpoint for reaching the backups bucket; set for a non-AWS S3-compatible store such as Cloudflare R2"`
	PgBackRestWebIdentityRoleARN   string `env:"XATA_PGBACKREST_S3_WEB_IDENTITY_ROLE_ARN" env-description:"AWS IRSA role that pgbackrest assumes for Branches with s3.keyType=web-id"`
	PgBackRestWorkloadIdentityPool string `env:"XATA_PGBACKREST_GCS_WORKLOAD_IDENTITY_POOL" env-description:"GKE Workload Identity pool (<project>.svc.id.goog); enables gcs.keyType=web-id for Branches"`
	PgBackRestGCSProjectNumber     string `env:"XATA_PGBACKREST_GCS_PROJECT_NUMBER" env-description:"number of the GCP project that owns the Workload Identity pool; required for managed folder IAM policies"`
	// When BackupsEndpoint is set (non-AWS S3-compatible store), backups
	// authenticate with static credentials from this Secret instead of an IAM
	// role. Defaults match the local-dev RustFS secret.
	BackupsCredentialsSecretName         string                     `env:"XATA_BACKUPS_CREDENTIALS_SECRET_NAME" env-default:"rustfs-eu" env-description:"Secret (in the clusters namespace) holding static S3 credentials, used when XATA_BACKUPS_ENDPOINT is set"`
	BackupsCredentialsAccessKeyIDKey     string                     `env:"XATA_BACKUPS_CREDENTIALS_ACCESS_KEY_ID_KEY" env-default:"accessKey" env-description:"key in the credentials Secret holding the access key ID"`
	BackupsCredentialsSecretAccessKeyKey string                     `env:"XATA_BACKUPS_CREDENTIALS_SECRET_ACCESS_KEY_KEY" env-default:"secretKey" env-description:"key in the credentials Secret holding the secret access key"`
	CloudProvider                        string                     `env:"XATA_CLOUD_PROVIDER" env-default:"aws" env-description:"cloud provider for per-Branch identity and backup credentials: aws or gcp"`
	Tolerations                          envcfg.TolerationListField `env:"XATA_CLUSTERS_TOLERATIONS" env-default:"xata.io/workload=dataplane:NoSchedule" env-description:"tolerations for cluster pods in the format key=value:effect"`
	EnforceZone                          bool                       `env:"XATA_ENFORCE_ZONE" env-default:"false" env-description:"enable zone-based pod anti-affinity for multi-instance clusters"`
	ImagePullSecrets                     []string                   `env:"XATA_IMAGE_SECRETS" env-default:"ghcr-secret" env-description:"image pull secrets for private PostgreSQL images"`
	XatastorEnabled                      bool                       `env:"XATA_XATASTOR_ENABLED" env-default:"true" env-description:"enable xatastor CSI integration for wakeup requests"`
	CSINodeNamespace                     string                     `env:"XATA_CSI_NODE_NAMESPACE" env-default:"xatastor" env-description:"namespace where CSI node plugin pods run"`
	CSINodePort                          int                        `env:"XATA_CSI_NODE_PORT" env-default:"50061" env-description:"port for the SlotController service on CSI node plugin pods"`
	WakeupRequestTTL                     time.Duration              `env:"XATA_WAKEUP_REQUEST_TTL" env-default:"60s" env-description:"time to keep completed WakeupRequests before deletion"`
	WakeupRPCTimeout                     time.Duration              `env:"XATA_WAKEUP_RPC_TIMEOUT" env-default:"30s" env-description:"timeout for the WakeUp RPC to the SlotController service on CSI node plugin pods"`
	WakeupPasswordSyncTimeout            time.Duration              `env:"XATA_WAKEUP_PASSWORD_SYNC_TIMEOUT" env-default:"5m" env-description:"how long a wakeup waits for CNPG to sync the xata role password before proceeding without it"`
	WakeupMaxConcurrent                  int                        `env:"XATA_WAKEUP_MAX_CONCURRENT" env-default:"128" env-description:"maximum concurrent wakeup reconciliations"`
	GCSFolderMaxConcurrent               int                        `env:"XATA_GCS_FOLDER_MAX_CONCURRENT" env-default:"4" env-description:"maximum concurrent managed folder reconciliations"`
	BranchMaxConcurrent                  int                        `env:"XATA_BRANCH_MAX_CONCURRENT" env-default:"1" env-description:"maximum concurrent Branch reconciliations (default 1 = current behavior)"`
	KubeClientQPS                        float64                    `env:"XATA_KUBE_CLIENT_QPS" env-default:"0" env-description:"Kubernetes API client QPS; 0 keeps the controller-runtime default (20)"`
	KubeClientBurst                      int                        `env:"XATA_KUBE_CLIENT_BURST" env-default:"0" env-description:"Kubernetes API client burst; 0 keeps the controller-runtime default (30)"`
}
