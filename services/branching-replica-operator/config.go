package branchingreplicaoperator

type Config struct {
	ClustersNamespace string `env:"XATA_CLUSTERS_NAMESPACE" env-default:"xata-clusters" env-description:"namespace the operator watches for BranchingReplica resources and creates managed resources in"`
}
