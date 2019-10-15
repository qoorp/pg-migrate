package pgmigrate

import (
	//	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/lib/pq"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// CreateMigration creates a new migration file with specified name
func (ctx *PGMigrate) CreateMigration(name string) error {
	ctx.dbg("CreateMigration", name)
	return ctx.migrationCreate(name)
}

// Finish commits lingering database transaction (if all in one transaction specified)
// and closes database handle.
func (ctx *PGMigrate) Finish() error {
	ctx.dbg("Finish")
	return ctx.finish()
}

// MigrateFromFile loads the specified file and does a direct migration without
// modifying the migrations table. Useful for database schema
// and database seeds.
func (ctx *PGMigrate) MigrateFromFile(fileName string) error {
	ctx.dbg("MigrateFromFile", fileName)
	return ctx.fileExec(fileName)
}

// New creates a new PGMigrate instance.
func New(config Config) *PGMigrate {
	return ctxNew(config)
}

// MigrateUp applies `up` migrations from migration dir in order.
// `steps` are number of migrations to perform.
func (ctx *PGMigrate) MigrateUp(steps int) error {
	ctx.dbg("MigrateUp", steps)
	migrations, err := ctx.migrationGetAll()
	if err != nil {
		return err
	}
	if err := ctx.dbMigrationsTableExist(); err != nil {
		ctx.dbg("MigrateUp", err)
		return err
	}
	migrated, err := ctx.dbGetMigrated()
	if err != nil {
		ctx.dbg("MigrateUp", err)
		return err
	}
	ss := ctx.migrationSuperSet(migrations, migrated)
	if len(ss) == 0 {
		ctx.logger.Print("there was nothing to migrate")
		return nil
	}
	stepsLeft := steps
	for _, m := range ss {
		ctx.dbg("stepsLeft", stepsLeft)
		if stepsLeft < 1 {
			break
		}
		if err := ctx.dbMigrate(m, migrateUP); err != nil {
			ctx.dbg("MigrateUp", err)
			return err
		}
		stepsLeft--
	}
	return nil
}

// MigrateDown applies `down` migrations from migration dir in order.
// `steps` are number of migrations to perform.
func (ctx *PGMigrate) MigrateDown(steps int) error {
	ctx.dbg("MigrateDown", steps)
	if err := ctx.dbMigrationsTableExist(); err != nil {
		return err
	}
	migratedVersions, err := ctx.dbGetMigrated()
	if err != nil {
		ctx.dbg("MigrateDown", err)
		return err
	}
	stepsLeft := steps
	for _, m := range migratedVersions {
		ctx.dbg("stepsLeft", stepsLeft)
		if stepsLeft < 1 {
			break
		}
		if err := ctx.dbMigrate(m, migrateDown); err != nil {
			ctx.dbg("MigrateDown", err)
			return err
		}
		stepsLeft--
	}
	return nil
}

// DumpDBSchema dumps the database schema without owner information.
func (ctx *PGMigrate) DumpDBSchema() ([]byte, error) {
	ctx.dbg("DumpDBSchema")
	cmd := exec.Command("pg_dump", ctx.config.DBUrl, "-s", "-O")
	var out bytes.Buffer
	cmd.Stdout = &out
	ctx.dbgJoin("Exec", "pg_dump", ctx.config.DBUrl, "-s", "-O")
	if err := cmd.Run(); err != nil {
		ctx.dbg("DumpDBSchema", err)
		return nil, err
	}
	return out.Bytes(), nil
}

// DumpDBSchemaToFileWithName calls `DumpDBSchema` and writes output to
// specified file.
func (ctx *PGMigrate) DumpDBSchemaToFileWithName(schemaName, migrationsName string) error {
	schema, err := ctx.DumpDBSchema()
	if err != nil {
		return err
	}
	migrations, err := ctx.dbGetMigrated()
	if err != nil {
		return err
	}
	for _, m := range migrations {
		m.Up = base64.StdEncoding.EncodeToString([]byte(m.Up))
		m.Down = base64.StdEncoding.EncodeToString([]byte(m.Down))
	}
	jBytes, err := json.Marshal(migrations)
	if err != nil {
		ctx.dbg("DumpDBSchemaToFileWithName", err)
		return err
	}
	if err := ctx.fileWriteContents(migrationsName, jBytes); err != nil {
		ctx.dbg("DumpDBSchemaToFileWithName", err)
		return err
	}
	if err := ctx.fileWriteContents(schemaName, schema); err != nil {
		ctx.dbg("DumpDBSchemaToFileWithName", err)
		return err
	}
	return nil
}

// DumpDBSchemaToFile dumps database schema and performed database migrations to files named `schema_<timestamp-unix>.sql` and `migrations_<timestamp-uni>.sql`.
func (ctx *PGMigrate) DumpDBSchemaToFile() error {
	now := time.Now().Unix()
	schemaName := fmt.Sprintf("schema_%d.sql", now)
	migrationsName := fmt.Sprintf("migrations_%d.sql", now)
	return ctx.DumpDBSchemaToFileWithName(schemaName, migrationsName)
}

// DumpDBFull dumps database schema and content to a file named `dump_<timestamp-unix>.sql`
func (ctx *PGMigrate) DumpDBFull() error {
	ctx.dbg("dumpDBData")
	now := time.Now().Unix()
	cmd := exec.Command("pg_dump", ctx.config.DBUrl, "-O", "--column-inserts")
	fp := filepath.Join(ctx.config.BaseDirectory, fmt.Sprintf("dump_%d.sql", now))
	file, err := os.OpenFile(fp, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		ctx.dbg("dumpDBData", err)
		return err
	}
	defer file.Close()
	r, w := io.Pipe()
	cmd.Stdout = w
	dc := make(chan error, 1)
	go func(w *io.PipeWriter, c chan error) {
		if err := cmd.Run(); err != nil {
			w.Close()
			c <- err
		}
		w.Close()
		c <- nil
	}(w, dc)
	io.Copy(file, r)
	if err := <-dc; err != nil {
		ctx.dbg("dumpDBData", err)
		return err
	}
	ctx.logger.Printf("database dump written to \"%s\"", fp)
	return nil
}

func (ctx *PGMigrate) LoadFullDump(dumpName string) error {
	ctx.dbg("LoadFullDump")
	opts, err := pq.ParseURL(ctx.config.DBUrl)
	if err != nil {
		ctx.dbg("LoadFullDump", err)
		return err
	}
	kvs := strings.Split(opts, " ")
	for i, _ := range kvs {
		kvs[i] = "--" + kvs[i]
	}
	fp := filepath.Join(ctx.config.BaseDirectory, dumpName)
	kvs = append(kvs, "--file="+fp)
	ctx.dbgJoin("psql", kvs...)
	cmd := exec.Command("psql", kvs...)
	if err := cmd.Run(); err != nil {
		ctx.dbg("LoadFullDump", err)
		return err
	}
	return nil
}
