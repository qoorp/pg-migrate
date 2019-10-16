package pgmigrate

type migrateDirection string

const (
	migrateUP   migrateDirection = "up"
	migrateDown migrateDirection = "down"
)

const (
	errMissingMigrationFilesTpl = "there are missing migration files: %v"
	errMissingMigrationFileTpl  = "missing migration file with version: %v"
	defaultMigrationsTable      = "pgmigrate"
)

type ConfirmCB func(prompt string) bool
