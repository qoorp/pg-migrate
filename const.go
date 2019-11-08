package pqmigrate

type migrateDirection string

const (
	migrateUp   migrateDirection = "up"
	migrateDown migrateDirection = "down"
)

const (
	errMissingMigrationFilesTpl = "there are missing migration files: %v"
	errMissingMigrationFileTpl  = "missing migration file with version: %v"
	defaultMigrationsTable      = "pqmigrate"
)

// ConfirmCB simple confirm function for potentially dangerous operations.
// if return value is true the operation will be performed, else abort.
type ConfirmCB func(prompt string) bool
