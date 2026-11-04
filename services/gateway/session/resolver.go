package session

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"xata/internal/xvalidator"

	"github.com/rs/zerolog/log"
)

var errMalformedHostname = errors.New("malformed hostname")

const (
	EndpointRW     = "rw"
	EndpointRO     = "ro"
	EndpointR      = "r"
	EndpointPooler = "pooler"
)

// deprecatedHostSuffix marks hostnames served in the deprecated branch
// connectionString field, so clients still using it can be detected.
const deprecatedHostSuffix = "-deprecated"

type BranchResolver interface {
	// Resolve maps a client-facing hostname to a concrete Branch. The
	// hostname may carry an explicit endpoint suffix, when it does not,
	// fallbackEndpoint is used.
	Resolve(ctx context.Context, serverName, fallbackEndpoint string) (*Branch, error)
}

type Branch struct {
	ID       string
	Address  string
	Endpoint string
}

type CNPGBranchResolver struct {
	cnpgNamespace  string
	port           int
	knownEndpoints map[string]bool
}

func NewCNPGBranchResolver(cnpgNamespace string, port int, poolerEnabled bool) *CNPGBranchResolver {
	return &CNPGBranchResolver{
		cnpgNamespace: cnpgNamespace,
		port:          port,
		knownEndpoints: map[string]bool{
			EndpointRW:     true,
			EndpointRO:     true,
			EndpointR:      true,
			EndpointPooler: poolerEnabled,
		},
	}
}

func (r *CNPGBranchResolver) Resolve(ctx context.Context, serverName, fallbackEndpoint string) (*Branch, error) {
	if !r.knownEndpoints[fallbackEndpoint] {
		fallbackEndpoint = EndpointRW
	}
	branchName, endpointType, deprecated, err := extractBranch(serverName, r.knownEndpoints, fallbackEndpoint)
	if err != nil {
		return nil, fmt.Errorf("read branch name: hostname [%s]: %w", serverName, err)
	}
	if deprecated {
		log.Ctx(ctx).Warn().
			Str("branch_id", branchName).
			Str("host", serverName).
			Msg("client connected using a hostname from the deprecated branch connectionString")
	}

	return &Branch{
		ID:       branchName,
		Address:  fmt.Sprintf("branch-%s-%s.%s.svc.cluster.local.:%d", branchName, endpointType, r.cnpgNamespace, r.port),
		Endpoint: endpointType,
	}, nil
}

func extractBranch(fullHostname string, knownEndpoints map[string]bool, fallbackEndpoint string) (string, string, bool, error) {
	hostnamePart, _, found := strings.Cut(fullHostname, ".")
	if !found || hostnamePart == "" {
		return "", "", false, errMalformedHostname
	}

	// The deprecated marker may sit on either side of an endpoint suffix,
	// clients append endpoints to the hostname they were given.
	hostnamePart, deprecated := strings.CutSuffix(hostnamePart, deprecatedHostSuffix)
	branchName, endpointType := parseEndpoint(hostnamePart, knownEndpoints, fallbackEndpoint)
	if !deprecated {
		branchName, deprecated = strings.CutSuffix(branchName, deprecatedHostSuffix)
	}

	if err := xvalidator.IsValidIdentifier(branchName); err != nil {
		return "", "", false, fmt.Errorf("invalid branch name: %w", err)
	}

	return branchName, endpointType, deprecated, nil
}

// parseEndpoint splits a hostname part into a branch name and endpoint type
// by checking whether the segment after the last hyphen is a known endpoint.
// If no known endpoint suffix is found, the full hostname is the branch name
// and the endpoint is fallbackEndpoint.
func parseEndpoint(hostnamePart string, knownEndpoints map[string]bool, fallbackEndpoint string) (string, string) {
	lastHyphen := strings.LastIndex(hostnamePart, "-")
	if lastHyphen > 0 {
		suffix := hostnamePart[lastHyphen+1:]
		if knownEndpoints[suffix] {
			return hostnamePart[:lastHyphen], suffix
		}
	}
	return hostnamePart, fallbackEndpoint
}

type ResolverFunc func(ctx context.Context, serverName, fallbackEndpoint string) (*Branch, error)

func (f ResolverFunc) Resolve(ctx context.Context, serverName, fallbackEndpoint string) (*Branch, error) {
	return f(ctx, serverName, fallbackEndpoint)
}
