package pgmigrate

type Config struct {
	AllInOneTx      bool
	BaseDirectory   string
	DBUrl           string
	Logger          Logger
	MigrationsTable string
	Debug           bool
}
