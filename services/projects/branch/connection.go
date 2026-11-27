package branch

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"xata/services/projects/store"

	clustersv1 "xata/gen/proto/clusters/v1"
)

// ConnectionString returns the branch's deprecated connection string (with
// sslmode=require kept for backwards compatibility). The caller may ignore the
// error: the connection string is eventually available after provisioning.
func (s *Service) ConnectionString(ctx context.Context, organizationID string, br *store.Branch) (string, error) {
	// TODO I believe eventually this must be its own API call (ie, we may support several managed users in the future)
	client, err := s.cells.GetCellConnection(ctx, organizationID, br.CellID)
	if err != nil {
		return "", err
	}
	defer client.Close()

	// get gateway host:port from the region
	region, err := s.store.GetRegion(ctx, organizationID, br.Region)
	if err != nil {
		return "", err
	}

	cell, err := s.store.GetCell(ctx, organizationID, br.CellID)
	if err != nil {
		return "", err
	}

	creds, err := client.GetPostgresClusterCredentials(ctx, &clustersv1.GetPostgresClusterCredentialsRequest{
		Id:       br.ID,
		Username: "app",
	})
	if err != nil {
		return "", err
	}

	// The deprecated marker keeps the connection routable while letting the
	// gateway log which clients still use this connection string.
	hostname, port, err := s.BranchEndpoint(region, cell.Subdomain, br.ID+deprecatedHostSuffix)
	if err != nil {
		return "", err
	}
	// The deprecated branch connectionString keeps sslmode for backwards
	// compatibility.
	return FormatConnectionString(creds.GetUsername(), creds.GetPassword(), hostname, port, DatabaseName(creds)) + "?sslmode=require", nil
}

// DatabaseName returns the database of the branch that owns creds. A cell
// that does not report one uses branchDatabaseName.
func DatabaseName(creds *clustersv1.GetPostgresClusterCredentialsResponse) string {
	if db := creds.GetDatabase(); db != "" {
		return db
	}
	return branchDatabaseName
}

// resolveGatewayHostPort returns the region's gateway host:port, falling back to
// the service default when the region does not override it.
func (s *Service) resolveGatewayHostPort(region *store.Region) string {
	if region.GatewayHostPort != "" {
		return region.GatewayHostPort
	}
	return s.defaultGatewayHostPort
}

// BranchEndpoint returns the hostname and port clients use to reach a branch
// through the region's gateway. hostLabel is normally the branch ID, optionally
// decorated with a suffix the gateway understands. A non-nil subdomain
// qualifies the hostname with the branch's cell (<label>.<subdomain>.<host>).
func (s *Service) BranchEndpoint(region *store.Region, subdomain *string, hostLabel string) (string, int, error) {
	hostPort := s.resolveGatewayHostPort(region)
	if hostPort == "" {
		return "", 0, errors.New("no gateway host:port configured")
	}

	if subdomain != nil {
		hostPort = *subdomain + "." + hostPort
	}

	// Regions may register a host-only gateway address (ie us-east-1.xata.tech),
	// in that case connections use the default postgres port.
	if !strings.Contains(hostPort, ":") {
		return hostLabel + "." + hostPort, defaultPostgresPort, nil
	}
	host, portStr, err := net.SplitHostPort(hostPort)
	if err != nil {
		return "", 0, fmt.Errorf("parse gateway host:port [%s]: %w", hostPort, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("parse gateway port [%s]: %w", portStr, err)
	}
	return hostLabel + "." + host, port, nil
}

// FormatConnectionString assembles the branch DSN from its parts. It carries no
// sslmode parameter, clients choose their own TLS settings.
func FormatConnectionString(username, password, hostname string, port int, database string) string {
	hostPort := hostname
	if port != defaultPostgresPort {
		hostPort = fmt.Sprintf("%s:%d", hostname, port)
	}
	return fmt.Sprintf("postgresql://%s:%s@%s/%s",
		username, password, hostPort, database)
}
