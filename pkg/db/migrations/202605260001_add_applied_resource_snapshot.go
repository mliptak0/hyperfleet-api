package migrations

import (
	"gorm.io/gorm"

	"github.com/go-gormigrate/gormigrate/v2"
)

func addAppliedResourceSnapshot() *gormigrate.Migration {
	return &gormigrate.Migration{
		ID: "202605260001",
		Migrate: func(tx *gorm.DB) error {
			// Add applied_resource_snapshot and applied_generation columns to adapter_statuses.
			// These fields support ADR-005 recreation strategy: adapters write the snapshot
			// after a successful apply, and the next reconciliation compares previous vs current
			// snapshots via a CEL expression to determine whether immutable fields changed.
			if err := tx.Exec(
				"ALTER TABLE adapter_statuses ADD COLUMN IF NOT EXISTS applied_resource_snapshot JSONB NULL;",
			).Error; err != nil {
				return err
			}
			if err := tx.Exec(
				"ALTER TABLE adapter_statuses ADD COLUMN IF NOT EXISTS applied_generation INT4 NULL;",
			).Error; err != nil {
				return err
			}
			return nil
		},
	}
}
