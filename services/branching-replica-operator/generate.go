package branchingreplicaoperator

//go:generate go run sigs.k8s.io/controller-tools/cmd/controller-gen object paths=./api/...
//go:generate go run sigs.k8s.io/controller-tools/cmd/controller-gen crd paths=./api/... output:crd:dir=../../charts/branching-replica-operator/crds
//go:generate go run sigs.k8s.io/controller-tools/cmd/controller-gen rbac:roleName=branching-replica-operator,fileName=role.yaml paths=./pkg/... output:rbac:dir=../../charts/branching-replica-operator/templates
