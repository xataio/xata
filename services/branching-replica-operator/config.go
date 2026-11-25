package branchingreplicaoperator

import "xata/internal/envcfg"

type Config struct {
	ClustersNamespace string                     `env:"XATA_CLUSTERS_NAMESPACE" env-default:"xata-clusters" env-description:"namespace the operator watches for BranchingReplica resources and creates managed resources in"`
	Tolerations       envcfg.TolerationListField `env:"XATA_CLUSTERS_TOLERATIONS" env-default:"xata.io/workload=dataplane:NoSchedule" env-description:"tolerations for cluster pods in the format key=value:effect"`
	ImagePullSecrets  []string                   `env:"XATA_IMAGE_SECRETS" env-default:"ghcr-secret" env-description:"image pull secrets for private PostgreSQL images"`
}
