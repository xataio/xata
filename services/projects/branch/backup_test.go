package branch

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClustersBackupConfig(t *testing.T) {
	tests := []struct {
		name           string
		backupConfig   *BackupConfiguration
		backupsEnabled bool
		wantSchedule   bool
		wantMethod     string
	}{
		{
			name:           "backups disabled returns config with BackupsEnabled false",
			backupConfig:   &BackupConfiguration{BackupTime: new("0:14:30")},
			backupsEnabled: false,
			wantSchedule:   false,
		},
		{
			name:           "backups enabled with no config returns default pgbackrest",
			backupConfig:   nil,
			backupsEnabled: true,
			wantSchedule:   true,
			wantMethod:     BackupMethodPgBackRest,
		},
		{
			name:           "backups enabled with config returns schedule pgbackrest",
			backupConfig:   &BackupConfiguration{BackupTime: new("0:14:30")},
			backupsEnabled: true,
			wantSchedule:   true,
			wantMethod:     BackupMethodPgBackRest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ClustersBackupConfig(tt.backupConfig, tt.backupsEnabled)

			require.Equal(t, tt.backupsEnabled, result.BackupsEnabled)

			if tt.wantSchedule {
				require.NotEmpty(t, result.BackupSchedule)
				require.NotEmpty(t, result.BackupRetention)
			} else {
				require.Empty(t, result.BackupSchedule)
				require.Empty(t, result.BackupRetention)
			}

			if tt.wantMethod != "" {
				require.Equal(t, tt.wantMethod, result.BackupMethod)
			}
		})
	}
}

func TestBackupRetentionDays(t *testing.T) {
	require.Equal(t, DefaultBackupRetentionPeriod, BackupRetentionDays(nil))
	require.Equal(t, DefaultBackupRetentionPeriod, BackupRetentionDays(&BackupConfiguration{}))
	require.Equal(t, DefaultBackupRetentionPeriod, BackupRetentionDays(&BackupConfiguration{RetentionPeriod: new(int32(0))}))
	require.Equal(t, 7, BackupRetentionDays(&BackupConfiguration{RetentionPeriod: new(int32(7))}))
}

func TestGenerateCron(t *testing.T) {
	tests := []struct {
		name     string
		schedule string
		expected string
	}{
		{
			name:     "daily backup at 2:30 PM",
			schedule: "*:14:30",
			expected: "0 30 14 * * *",
		},
		{
			name:     "Sunday backup at 11:45 PM",
			schedule: "0:23:45",
			expected: "0 45 23 * * 0",
		},
		{
			name:     "Monday backup at 6:15 AM",
			schedule: "1:06:15",
			expected: "0 15 06 * * 1",
		},
		{
			name:     "Tuesday backup at 3:00 AM",
			schedule: "2:03:00",
			expected: "0 00 03 * * 2",
		},
		{
			name:     "Wednesday backup at midnight",
			schedule: "3:00:00",
			expected: "0 00 00 * * 3",
		},
		{
			name:     "Thursday backup with single digit minute",
			schedule: "4:12:05",
			expected: "0 05 12 * * 4",
		},
		{
			name:     "Friday backup with single digit hour",
			schedule: "5:09:30",
			expected: "0 30 09 * * 5",
		},
		{
			name:     "Saturday backup at 23:59",
			schedule: "6:23:59",
			expected: "0 59 23 * * 6",
		},
		{
			name:     "daily backup at midnight",
			schedule: "*:00:00",
			expected: "0 00 00 * * *",
		},
		{
			name:     "daily backup at noon",
			schedule: "*:12:00",
			expected: "0 00 12 * * *",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GenerateCron(tt.schedule)
			assert.Equal(t, tt.expected, result)
		})
	}
}
