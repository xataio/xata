package main

import (
	"fmt"
	"os"

	"xata/internal/cmd"
	branchingreplicaoperator "xata/services/branching-replica-operator"
)

func main() {
	svc := branchingreplicaoperator.NewBranchingReplicaOperatorService()
	if err := cmd.RootCmdForService(svc).Execute(); err != nil {
		fmt.Println(err) //nolint:forbidigo
		os.Exit(1)
	}
}
