package clusterpooloperator

//go:generate go run sigs.k8s.io/controller-tools/cmd/controller-gen object paths=./api/...
//go:generate go run sigs.k8s.io/controller-tools/cmd/controller-gen crd paths=./api/... output:crd:dir=../../saas-charts/clusterpool-operator/crds
//go:generate go run sigs.k8s.io/controller-tools/cmd/controller-gen rbac:roleName=clusterpool-operator,fileName=role.yaml paths=../../saas-services/clusterpool-operator/pkg/reconciler output:rbac:dir=../../saas-charts/clusterpool-operator/files
