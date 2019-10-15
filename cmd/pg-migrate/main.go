package main

import (
	"fmt"
	pgmigrate "github.com/Preciselyco/pg-migrate"
	"github.com/docopt/docopt-go"
	"github.com/happierall/l"
	"github.com/joho/godotenv"
	"os"
	"path/filepath"
	"strconv"
)

var arguments = map[string]interface{}{}
var logger = l.New()

func main() {
	usage := `pg-migrate

Usage:
  pg-migrate up [--url=<url>] [--dir=<dir>] [--steps=<steps>] [--bw]
  pg-migrate down [--url=<url>] [--dir=<dir>] [--steps=<steps>] [--bw] 
  pg-migrate create <name> [--bw]
  pg-migrate dump-schema [--dir=<dir>]
  pg-migrate dump-full [--dir=<dir>]
  pg-migrate load-schema [--dir=<dir>]
  pg-migrate load-dump <name> [--dir=<dir>]
  pg-migrate seed [--dir=<dir>]
  pg-migrate -h | --help
  pg-migrate --version

Options:
  -h --help        Show help.
  --version        Show version.
  --dir=<dir>      Directory where migrations files are stores. [default: pgmigrate/]
  --steps=<steps>  Max steps to migrate [default: 1].
  --bw             No colour (black and white).
`
	err := godotenv.Load()
	if err != nil {
		l.Warn("no .env file")
	}
	arguments, _ = docopt.ParseDoc(usage)
	if arguments["--bw"].(bool) {
		logger.Production = true
	}
	if arguments["up"].(bool) {
		logger.Print("migrating up...")
		if err := upCMD(); err != nil {
			logger.Error(err)
			return
		}
	} else if arguments["down"].(bool) {
		logger.Print("migrating down...")
		if err := downCMD(); err != nil {
			logger.Error(err)
			return
		}
	} else if arguments["create"].(bool) {
		logger.Print("creating new migration files...")
		migrationName, ok := arguments["<name>"].(string)
		if !ok {
			logger.Error("could parse migration name to string")
			return
		}
		if err := createCMD(migrationName); err != nil {
			logger.Error(err)
			return
		}
	} else if arguments["dump-schema"].(bool) {
		logger.Print("dumping database schema...")
		if err := dumpSchemaCMD(); err != nil {
			logger.Error(err)
			return
		}
	} else if arguments["dump-full"].(bool) {
		logger.Print("dumping full db...")
		if err := dumpFullCMD(); err != nil {
			logger.Error(err)
			return
		}
	} else if arguments["load-schema"].(bool) {
		logger.Print("loading sql schema...")
		if err := loadSchemaCMD(); err != nil {
			logger.Error(err)
			return
		}
	} else if arguments["load-dump"].(bool) {
		dumpName := arguments["<name>"].(string)
		if err := loadDumpCMD(dumpName); err != nil {
			logger.Error(err)
			return
		}
	} else if arguments["seed"].(bool) {
		logger.Print("seeding db...")
		if err := seedCMD(); err != nil {
			logger.Error(err)
			return
		}
	}
	logger.Print("Success!")
}

func getSteps() int {
	steps, err := strconv.Atoi(arguments["--steps"].(string))
	if err != nil {
		return 1
	}
	return steps
}

func getEnvOrDefaultBool(envKey string, def bool) bool {
	v, err := strconv.ParseBool(os.Getenv(envKey))
	if err != nil {
		return def
	}
	return v
}

func getConfig() (pgmigrate.Config, error) {
	cfg := pgmigrate.Config{}
	cfg.DBUrl = os.Getenv("PGM_DATABASE_URL")
	if u, found := arguments["<url>"]; found {
		cfg.DBUrl = u.(string)
	}
	if cfg.DBUrl == "" {
		return cfg, fmt.Errorf("no database url provided")
	}
	cfg.BaseDirectory = os.Getenv("PGM_BASE_DIRECTORY")
	if d, found := arguments["--dir"]; found {
		var fullDir string
		var err error
		fullDir, err = filepath.Abs(d.(string))
		if err != nil {
			return cfg, fmt.Errorf("could not get full path dir: %v", err)
		}
		cfg.BaseDirectory = fullDir
	}
	cfg.Debug = getEnvOrDefaultBool("PGM_DEBUG", false)
	cfg.AllInOneTx = getEnvOrDefaultBool("PGM_ALL_IN_ONE_TX", false)
	cfg.Logger = logger
	cfg.MigrationsTable = os.Getenv("PGM_MIGRATIONS_TABLE")
	return cfg, nil
}

func getConfigOrDie() pgmigrate.Config {
	cfg, err := getConfig()
	if err != nil {
		l.Error("could not get a valid config")
		l.Error(err)
		os.Exit(-1)
	}
	return cfg
}

func createCMD(name string) error {
	ctx := pgmigrate.New(getConfigOrDie())
	return ctx.CreateMigration(name)
}

func upCMD() error {
	steps := getSteps()
	ctx := pgmigrate.New(getConfigOrDie())
	if err := ctx.MigrateUp(steps); err != nil {
		return err
	}
	return ctx.Finish()
}

func downCMD() error {
	steps := getSteps()
	ctx := pgmigrate.New(getConfigOrDie())
	if err := ctx.MigrateDown(steps); err != nil {
		return err
	}
	return ctx.Finish()
}

func dumpSchemaCMD() error {
	ctx := pgmigrate.New(getConfigOrDie())
	return ctx.DumpDBSchemaToFile()
}

func dumpFullCMD() error {
	ctx := pgmigrate.New(getConfigOrDie())
	return ctx.DumpDBFull()
}

func loadSchemaCMD() error {
	ctx := pgmigrate.New(getConfigOrDie())
	if err := ctx.MigrateFromFile("schema.sql"); err != nil {
		return err
	}
	return ctx.Finish()
}

func loadDumpCMD(dumpName string) error {
	ctx := pgmigrate.New(getConfigOrDie())
	if err := ctx.LoadFullDump(dumpName); err != nil {
		return err
	}
	return ctx.Finish()
}

func seedCMD() error {
	ctx := pgmigrate.New(getConfigOrDie())
	if err := ctx.MigrateFromFile("seeds.sql"); err != nil {
		return err
	}
	return ctx.Finish()
}
