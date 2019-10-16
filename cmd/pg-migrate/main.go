package main

import (
	"bufio"
	"fmt"
	pgmigrate "github.com/Preciselyco/pg-migrate"
	"github.com/docopt/docopt-go"
	"github.com/fatih/color"
	"github.com/joho/godotenv"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var arguments = map[string]interface{}{}
var logger = newCmdLogger()
var bw = false

const (
	argURL    = "--url"
	argDIR    = "--dir"
	argName   = "--name"
	argSteps  = "--steps"
	argBW     = "--bw"
	argDryRun = "-d"

	confirmY       = "y"
	confirmPainful = "yes-i-am-really-really-sure"
)

var cmdKeys = []string{"create-db", "drop-db", "up", "down", "sync", "create", "dump-schema", "dump-full", "load-schema", "load-dump", "seed"}

var cmds = map[string]struct {
	f func() error
	d string
}{
	"create-db":   {f: createDbCMD, d: "initializing db"},
	"drop-db":     {f: dropDbCMD, d: "dropping database"},
	"up":          {f: upCMD, d: "migrating up"},
	"down":        {f: downCMD, d: "migrating down"},
	"sync":        {f: syncCMD, d: "syncing database and filesystem"},
	"create":      {f: createCMD, d: "creating migration files"},
	"dump-schema": {f: dumpSchemaCMD, d: "dumping database schema"},
	"dump-full":   {f: dumpFullCMD, d: "dumping database and content"},
	"load-schema": {f: loadSchemaCMD, d: "loading database schema"},
	"load-dump":   {f: loadDumpCMD, d: "loading database dump"},
	"seed":        {f: seedCMD, d: "seeding database"},
}

func main() {
	usage := `pg-migrate

Usage:
  pg-migrate create-db [--url=<url>] [--bw]
  pg-migrate drop-db [--url=<url>] [--bw]
  pg-migrate up [--url=<url>] [--dir=<dir>] [--steps=<steps>] [--bw] [-d]
  pg-migrate down [--url=<url>] [--dir=<dir>] [--steps=<steps>] [--bw] [-d]
  pg-migrate sync [--url=<url>] [--dir=<dir>] [--bw] [-d]
  pg-migrate create <name> [--dir=<dir>] [--bw] [-d]
  pg-migrate dump-schema [--dir=<dir>] [--name=<name>] [--bw]
  pg-migrate dump-full [--dir=<dir>] [--name=<name>] [--bw]
  pg-migrate load-schema [--dir=<dir>] [--name=<name>] [--bw] [-d]
  pg-migrate load-dump <name> [--dir=<dir>] [--name=<name>] [--bw] [-d]
  pg-migrate seed [--dir=<dir>] [--name=<name>] [--bw] [-d]
  pg-migrate -h | --help

Options:
  -h --help        Show help.
  --version        Show version.
  --dir=<dir>      Directory where migrations files are stores. [default: pgmigrate/]
  --steps=<steps>  Max steps to migrate [default: 1].
  --bw             No colour (black and white). [default false]
  -d               Dry run. [default: false]
`
	arguments, _ = docopt.ParseDoc(usage)
	bw = arguments[argBW].(bool)
	err := godotenv.Load()
	if err != nil {
		logger.Warn("No .env file loaded")
	}
	for _, k := range cmdKeys {
		if c, ok := arguments[k].(bool); c && ok {
			if cmd, found := cmds[k]; found {
				logger.Inf("%s", cmd.d+"...")
				if err := cmd.f(); err != nil {
					logger.Error(err)
				}
			} else {
				logger.Error(fmt.Sprintf("command: '%s' not implemented", k))
			}
		} else if !c && !ok {
			logger.Error(fmt.Sprintf("command: '%s' not implemented", k))
		}
	}
	logger.Ok("Success!")
}

type cmdLogger struct{}

func newCmdLogger() *cmdLogger {
	return &cmdLogger{}
}

func (l *cmdLogger) Printf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}

func (l *cmdLogger) printFEach(c func(string, ...interface{}), format string, args ...interface{}) {
	for _, v := range args {
		if bw || c == nil {
			l.Printf(format, v)
		} else {
			c(format, v)
		}
	}
}

func (l *cmdLogger) Print(args ...interface{}) {
	l.printFEach(nil, "%v\n", args...)
}

func (l *cmdLogger) Error(args ...interface{}) {
	l.printFEach(color.Red, "ERROR: %v\n", args...)
}

func (l *cmdLogger) Warn(args ...interface{}) {
	l.printFEach(color.Yellow, "%v\n", args...)
}

func (l *cmdLogger) Inf(args ...interface{}) {
	l.printFEach(color.Cyan, "%v\n", args...)
}

func (l *cmdLogger) Ok(args ...interface{}) {
	l.printFEach(color.Green, "%v\n", args...)
}

func (l *cmdLogger) DBG(args ...interface{}) {
	l.printFEach(color.Magenta, "DBG: %v\n", args...)
}

func getSteps() int {
	if sVal, ok := arguments[argSteps].(string); ok {
		steps, err := strconv.Atoi(sVal)
		if err != nil {
			// gratuitous goto
			goto return_one
		}
		return steps
	}
return_one:
	return 1
}

func getEnvOrDefaultBool(envKey string, def bool) bool {
	v, err := strconv.ParseBool(os.Getenv(envKey))
	if err != nil {
		return def
	}
	return v
}

func confirmCB(expected string, simple bool) func(prompt string) bool {
	return func(prompt string) bool {
		reader := bufio.NewReader(os.Stdin)
		replacement := ""
		replacer := strings.NewReplacer(
			"\r\n", replacement,
			"\r", replacement,
			"\n", replacement,
			"\v", replacement,
			"\f", replacement,
			"\u0085", replacement,
			"\u2028", replacement,
			"\u2029", replacement,
		)
		if simple {
			logger.Warn(fmt.Sprintf("%s: [y/N] ", prompt))
			text, _ := reader.ReadString('\n')
			if text == "" {
				return false
			}
			return replacer.Replace(strings.ToLower(text)) == expected
		}
		logger.Warn(fmt.Sprintf("%s: [%s/N] ", prompt, expected))
		text, _ := reader.ReadString('\n')
		return replacer.Replace(text) == expected
	}
}

func getConfig() (pgmigrate.Config, error) {
	logger.DBG(fmt.Sprintf("%+v\n", arguments))
	cfg := pgmigrate.Config{}
	cfg.DBUrl = os.Getenv("PGM_DATABASE_URL")
	if u, ok := arguments[argURL].(string); ok {
		cfg.DBUrl = u
	}
	if cfg.DBUrl == "" {
		return cfg, fmt.Errorf("no database url provided")
	}
	cfg.BaseDirectory = os.Getenv("PGM_BASE_DIRECTORY")
	if d, ok := arguments[argDIR].(string); ok {
		var fullDir string
		var err error
		fullDir, err = filepath.Abs(d)
		if err != nil {
			return cfg, fmt.Errorf("could not get full path dir: %v", err)
		}
		cfg.BaseDirectory = fullDir
	}
	cfg.Debug = getEnvOrDefaultBool("PGM_DEBUG", false)
	cfg.AllInOneTx = getEnvOrDefaultBool("PGM_ALL_IN_ONE_TX", false)
	cfg.Logger = logger
	cfg.MigrationsTable = os.Getenv("PGM_MIGRATIONS_TABLE")
	cfg.DryRun = false
	if dr, ok := arguments[argDryRun].(bool); ok {
		cfg.DryRun = dr
	}
	logger.DBG(fmt.Sprintf("%+v", cfg))
	return cfg, nil
}

func getConfigOrDie() pgmigrate.Config {
	cfg, err := getConfig()
	if err != nil {
		logger.Error("could not get a valid config")
		logger.Error(err)
		os.Exit(-1)
	}
	return cfg
}

func getArgStringOrNil(key string) *string {
	if d, found := arguments[key]; found {
		if dVal, ok := d.(string); ok {
			val := new(string)
			*val = dVal
			return val
		}
	}
	return nil
}

func createDbCMD() error {
	ctx := pgmigrate.New(getConfigOrDie())
	return ctx.CreateDB(confirmCB(confirmY, true))
}

func dropDbCMD() error {
	ctx := pgmigrate.New(getConfigOrDie())
	return ctx.DropDB(confirmCB(confirmPainful, false))
}

func createCMD() error {
	ctx := pgmigrate.New(getConfigOrDie())
	name := getArgStringOrNil("<name>")
	if name == nil {
		return fmt.Errorf("<name> required")
	}
	return ctx.CreateMigration(*name)
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

func syncCMD() error {
	ctx := pgmigrate.New(getConfigOrDie())
	return ctx.Sync()
}

func dumpSchemaCMD() error {
	fileName := getArgStringOrNil(argName)
	ctx := pgmigrate.New(getConfigOrDie())
	return ctx.DumpDBSchemaToFile(fileName)
}

func dumpFullCMD() error {
	fileName := getArgStringOrNil(argName)
	ctx := pgmigrate.New(getConfigOrDie())
	return ctx.DumpDBFull(fileName)
}

func loadSchemaCMD() error {
	ctx := pgmigrate.New(getConfigOrDie())
	fileName := "schema.sql"
	if fn := getArgStringOrNil(argName); fn != nil {
		fileName = *fn
	}
	if err := ctx.MigrateFromFile(fileName); err != nil {
		return err
	}
	return ctx.Finish()
}

func loadDumpCMD() error {
	ctx := pgmigrate.New(getConfigOrDie())
	dumpName := getArgStringOrNil("<name>")
	if dumpName == nil {
		return fmt.Errorf("<name> required")
	}
	if err := ctx.LoadFullDump(*dumpName); err != nil {
		return err
	}
	return ctx.Finish()
}

func seedCMD() error {
	ctx := pgmigrate.New(getConfigOrDie())
	fileName := "seeds.sql"
	if fn := getArgStringOrNil(argName); fn != nil {
		fileName = *fn
	}
	if err := ctx.MigrateFromFile(fileName); err != nil {
		return err
	}
	return ctx.Finish()
}
