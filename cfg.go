package pqmigrate

// Config options for the library
type Config struct {
	AllInOneTx      bool   // Perform all database operations in the same transaction
	BaseDirectory   string // Directory where the sql files are stored
	DBUrl           string // Postgresql url `psql://<user>:<pwd>@<host>:<port>/<db_name>`
	Logger          Logger // If set the logger will be used instead of standard out
	MigrationsTable string // Name of migrations table in database
	Debug           bool   // Show debug info
	DryRun          bool   // Perform all database operations but don't commit
}
