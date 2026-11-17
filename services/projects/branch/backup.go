package branch

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"regexp"
	"strings"

	clustersv1 "xata/gen/proto/clusters/v1"
)

const defaultBackupSchedule = "0 0 0 * * 0"

const (
	// MinRetentionPeriod and MaxRetentionPeriod bound the backup retention period
	// (in days) a caller may request. Exported so the api validator delegates
	// here instead of keeping its own copy.
	MinRetentionPeriod = 2
	MaxRetentionPeriod = 35
)

var backupTimePattern = regexp.MustCompile(`^(\*|[0-6]):(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9])$`)

// ValidateBackup rejects a malformed backup configuration before it reaches the
// cron/retention conversion. It is the shared guard for every caller of
// Provision: without it a bad BackupTime would panic GenerateCron and an
// out-of-range retention would be forwarded to the provisioner. The api backup
// validator delegates here.
func ValidateBackup(branchName string, c *BackupConfiguration) error {
	if c == nil {
		return nil
	}
	if c.RetentionPeriod != nil && (*c.RetentionPeriod < MinRetentionPeriod || *c.RetentionPeriod > MaxRetentionPeriod) {
		return ErrInvalidParam{BranchName: branchName, Param: "backup retentionPeriod", Message: fmt.Sprintf("must be at least %d days and maximum %d days", MinRetentionPeriod, MaxRetentionPeriod)}
	}
	if c.BackupTime != nil && !isValidBackupTimeFormat(*c.BackupTime) {
		return ErrInvalidParam{BranchName: branchName, Param: "backup time", Message: fmt.Sprintf("invalid backup time format '%s', must match format 'D:HH:MM' where D= * or 0-6, HH=00-23, MM=00-59", *c.BackupTime)}
	}
	return nil
}

// isValidBackupTimeFormat reports whether s matches "D:HH:MM" where D is * or
// 0-6 (day of week), HH is 00-23, and MM is 00-59.
func isValidBackupTimeFormat(s string) bool {
	return backupTimePattern.MatchString(strings.TrimSpace(s))
}

// BackupRetentionDays returns the retention period in days for the given backup
// configuration, defaulting when unset. It is exported so the api adapters can
// delegate to it instead of keeping a copy.
func BackupRetentionDays(backupConfig *BackupConfiguration) int {
	if backupConfig == nil || backupConfig.RetentionPeriod == nil || *backupConfig.RetentionPeriod == 0 {
		return DefaultBackupRetentionPeriod
	}
	return int(*backupConfig.RetentionPeriod)
}

// ClustersBackupConfig builds the clusters backup configuration from the neutral
// backup configuration and the region's backups-enabled flag. pgbackrest is the
// only backup method; the barman-cloud plugin is no longer deployed. It is
// exported so the api adapters can delegate to it instead of keeping a copy.
func ClustersBackupConfig(backupConfig *BackupConfiguration, backupsEnabled bool) *clustersv1.BackupConfiguration {
	// if backups are disabled, return a disabled configuration
	if !backupsEnabled {
		return &clustersv1.BackupConfiguration{
			BackupsEnabled: backupsEnabled,
		}
	}

	// if nothing was set via the API request, use defaults = weekly, sunday random
	if backupConfig == nil {
		return &clustersv1.BackupConfiguration{
			BackupSchedule:  GenerateRandomBackupCron(),
			BackupRetention: fmt.Sprintf("%dd", DefaultBackupRetentionPeriod),
			BackupsEnabled:  backupsEnabled,
			BackupMethod:    BackupMethodPgBackRest,
		}
	}

	backupConfiguration := clustersv1.BackupConfiguration{
		BackupsEnabled: backupsEnabled,
		BackupMethod:   BackupMethodPgBackRest,
	}
	if backupConfig.BackupTime == nil || *backupConfig.BackupTime == "" {
		backupConfiguration.BackupSchedule = GenerateRandomBackupCron()
	} else {
		backupConfiguration.BackupSchedule = GenerateCron(*backupConfig.BackupTime)
	}

	if backupConfig.RetentionPeriod == nil || *backupConfig.RetentionPeriod == 0 {
		backupConfiguration.BackupRetention = fmt.Sprintf("%dd", DefaultBackupRetentionPeriod)
	} else {
		backupConfiguration.BackupRetention = fmt.Sprintf("%dd", *backupConfig.RetentionPeriod)
	}
	return &backupConfiguration
}

func GenerateRandomBackupCron() string {
	// Random hour between 2-5 (for 2:00-5:59 AM range)
	hour, err := rand.Int(rand.Reader, big.NewInt(4))
	if err != nil {
		return defaultBackupSchedule
	}
	randomHour := int(hour.Int64()) + 2

	// Random minute between 0-59
	minute, err := rand.Int(rand.Reader, big.NewInt(60))
	if err != nil {
		return defaultBackupSchedule
	}
	randomMinute := int(minute.Int64())

	// robfig/cron format: "second minute hour day-of-month month day-of-week"
	// 0 = Sunday in cron format
	return fmt.Sprintf("0 %d %d * * 0", randomMinute, randomHour)
}

// GenerateCron converts a schedule string "d:hh:mm" to robfig/cron format
// "second minute hour day-of-month month day-of-week".
func GenerateCron(schedule string) string {
	parts := strings.SplitN(schedule, ":", 3)
	if len(parts) != 3 {
		// Callers validate via ValidateBackup before reaching here; guard anyway
		// so a malformed value returns the default instead of panicking.
		return defaultBackupSchedule
	}
	d, hh, mm := parts[0], parts[1], parts[2]

	if d == "*" {
		return "0 " + mm + " " + hh + " * * *"
	}
	return "0 " + mm + " " + hh + " * * " + d
}
