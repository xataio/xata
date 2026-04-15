package events

import (
	"sort"
	"time"
)

type Event struct {
	Name       string
	Properties map[string]any
	OrgID      string
	Timestamp  time.Time
}

func NewOrganizationCreatedEvent(organizationID string) Event {
	return Event{
		Name:  "organization created",
		OrgID: organizationID,
		Properties: map[string]any{
			"organization": organizationID,
		},
	}
}

func NewProjectCreatedEvent(organizationID, projectID string) Event {
	return Event{
		Name:  "project created",
		OrgID: organizationID,
		Properties: map[string]any{
			"organization": organizationID,
			"project":      projectID,
		},
	}
}

func NewProjectDeletedEvent(organizationID, projectID string) Event {
	return Event{
		Name:  "project deleted",
		OrgID: organizationID,
		Properties: map[string]any{
			"organization": organizationID,
			"project":      projectID,
		},
	}
}

func NewBranchFromConfigurationEvent(organizationID, projectID, branchID, region string, image, instanceType string, replicas int, storageSize *int32) Event {
	props := map[string]any{
		"organization":  organizationID,
		"project":       projectID,
		"branch":        branchID,
		"region":        region,
		"child_branch":  false,
		"image":         image,
		"instance_type": instanceType,
		"replicas":      replicas,
	}

	if storageSize != nil {
		props["storage_size"] = int(*storageSize)
	}

	return Event{
		Name:       "branch created",
		OrgID:      organizationID,
		Properties: props,
	}
}

func NewBranchFromParentEvent(organizationID, projectID, parentID, branchID, region string) Event {
	return Event{
		Name:  "branch created",
		OrgID: organizationID,
		Properties: map[string]any{
			"organization": organizationID,
			"project":      projectID,
			"branch":       branchID,
			"region":       region,
			"parent_id":    parentID,
			"child_branch": true,
		},
	}
}

func NewBranchDeletedEvent(organizationID, projectID, branchID string) Event {
	return Event{
		Name:  "branch deleted",
		OrgID: organizationID,
		Properties: map[string]any{
			"organization": organizationID,
			"project":      projectID,
			"branch":       branchID,
		},
	}
}

func NewProjectUpdatedEvent(organizationID, projectID string, changedFields []string, newValues map[string]any) Event {
	return Event{
		Name:  "project updated",
		OrgID: organizationID,
		Properties: map[string]any{
			"organization":   organizationID,
			"project":        projectID,
			"changed_fields": changedFields,
			"new_values":     newValues,
		},
	}
}

func NewBranchUpdatedEvent(organizationID, projectID, branchID string, changedFields []string, newValues map[string]any) Event {
	return Event{
		Name:  "branch updated",
		OrgID: organizationID,
		Properties: map[string]any{
			"organization":   organizationID,
			"project":        projectID,
			"branch":         branchID,
			"changed_fields": changedFields,
			"new_values":     newValues,
		},
	}
}

func NewPaymentMethodAttachedEvent(organizationID, provider string) Event {
	return Event{
		Name:  "payment method attached",
		OrgID: organizationID,
		Properties: map[string]any{
			"organization": organizationID,
			"provider":     provider,
		},
	}
}

func NewInvoicePaidEvent(organizationID, marketplace, currency string, amountDue, total float64, paidAt time.Time) Event {
	return Event{
		Name:      "invoice paid",
		OrgID:     organizationID,
		Timestamp: paidAt,
		Properties: map[string]any{
			"organization": organizationID,
			"marketplace":  marketplace,
			"amount_due":   amountDue,
			"total":        total,
			"currency":     currency,
		},
	}
}

func NewMemberInvitedEvent(organizationID, email string) Event {
	return Event{
		Name:  "member invited",
		OrgID: organizationID,
		Properties: map[string]any{
			"organization":  organizationID,
			"invitee_email": email,
		},
	}
}

func NewBranchRestoredFromBackupEvent(organizationID, projectID, sourceBranchID, newBranchID string) Event {
	return Event{
		Name:  "branch restored from backup",
		OrgID: organizationID,
		Properties: map[string]any{
			"organization":    organizationID,
			"project":         projectID,
			"source_branch":   sourceBranchID,
			"restored_branch": newBranchID,
		},
	}
}

// BranchCreationSummaryMetrics is used for synthetic PostHog summary events,
// not events generated directly by user interactions.
type BranchCreationSummaryMetrics struct {
	TotalBranchesAllTime      int
	AiBranchesAllTime         int
	NonConsoleBranchesAllTime int
	CliBranchesAllTime        int
	CiBranchesAllTime         int
	TotalBranches7day         int
	AiBranches7day            int
	NonConsoleBranches7day    int
	CliBranches7day           int
	CiBranches7day            int
}

// This is a summary event that we generate from data warehouse data
func NewBranchCreationSummaryEvent(organizationID string, metrics BranchCreationSummaryMetrics, timestamp time.Time) Event {
	return Event{
		Name:      "summary: branch creation",
		OrgID:     organizationID,
		Timestamp: timestamp,
		Properties: map[string]any{
			"organization":              organizationID,
			"totalBranchesAllTime":      metrics.TotalBranchesAllTime,
			"aiBranchesAllTime":         metrics.AiBranchesAllTime,
			"nonConsoleBranchesAllTime": metrics.NonConsoleBranchesAllTime,
			"cliBranchesAllTime":        metrics.CliBranchesAllTime,
			"ciBranchesAllTime":         metrics.CiBranchesAllTime,
			"totalBranches7day":         metrics.TotalBranches7day,
			"aiBranches7day":            metrics.AiBranches7day,
			"nonConsoleBranches7day":    metrics.NonConsoleBranches7day,
			"cliBranches7day":           metrics.CliBranches7day,
			"ciBranches7day":            metrics.CiBranches7day,
		},
	}
}

// CostSummaryMetric is used for synthetic PostHog summary events,
// not events generated directly by user interactions.
type CostSummaryMetric struct {
	AllTime     float64
	SevenDay    float64
	CostAllTime float64
	Cost7day    float64
}

// This is a summary event that we generate from data warehouse data
func NewCostSummaryEvent(organizationID string, metrics map[string]CostSummaryMetric, timestamp time.Time) Event {
	properties := map[string]any{
		"organization": organizationID,
	}

	// This event creates properties for multiple orb "billable metrics" so that as we extend billing system to
	// add more billable metrics they will automatically be added to this event
	metricNames := make([]string, 0, len(metrics))
	for metricName := range metrics {
		metricNames = append(metricNames, metricName)
	}
	sort.Strings(metricNames)

	for _, metricName := range metricNames {
		properties[metricName+"AllTime"] = metrics[metricName].AllTime
		properties[metricName+"7day"] = metrics[metricName].SevenDay
		properties[metricName+"CostAllTime"] = metrics[metricName].CostAllTime
		properties[metricName+"Cost7day"] = metrics[metricName].Cost7day
	}

	return Event{
		Name:       "summary: cost",
		OrgID:      organizationID,
		Timestamp:  timestamp,
		Properties: properties,
	}
}
