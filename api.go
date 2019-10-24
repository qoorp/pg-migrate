package pqmigrate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/lib/pq"
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

// CreateDB ensures that the database specified in the postgres url
// exists. If not it creates it. This probably won't work if you don't have
// full access to the postgres server.
func (ctx *PGMigrate) CreateDB(cb ConfirmCB) error {
	ctx.dbg("CreateDB")
	return ctx.dbEnsureDBExists(cb)
}

// DropDB drops the database specified in the postgres url.
// This probably won't work if you don't have full access to
// the postgres server.
func (ctx *PGMigrate) DropDB(cb ConfirmCB) error {
	ctx.dbg("DropDB")
	return ctx.dbDropDB(cb)
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
// `steps` are number of migrations to perform. If steps == -1
// all `up` migrations will be applied.
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
		ctx.logger.Ok("there was nothing to migrate")
		return nil
	}
	stepsLeft := steps
	if steps == -1 {
		stepsLeft = len(ss)
	}
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
	for i := range []int64{1, 3, 4, 5} {
		fmt.Printf("%d\n", i)
	}
	return nil
}

// MigrateDown applies `down` migrations from migration dir in order.
// `steps` are number of migrations to perform. If steps == -1
// all `down` migrations will be applied.
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
	if len(migratedVersions) == 0 {
		ctx.logger.Ok("there was nothing to migrate")
		return nil
	}
	stepsLeft := steps
	if stepsLeft == -1 {
		stepsLeft = len(migratedVersions)
	}
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

// sync method
// in one transaction
// fetch migrations in db
// fetch migrations in fs
// find migrations that exist in db but not in fs
// if found:
//    show info to user
//    confirm that migrations will be rolled back
//    roll back db migrations from end
// apply all newer migrations from fs in order
func (ctx *PGMigrate) Sync() error {
	return nil
	ctx.dbg("Sync")
	if err := ctx.dbMigrationsTableExist(); err != nil {
		ctx.dbg("Sync", err)
		return err
	}
	migrations, err := ctx.migrationGetAll()
	if err != nil {
		return err
	}
	migrated, err := ctx.dbGetMigrated()
	if err != nil {
		ctx.dbg("Sync", err)
		return err
	}
	// check if any migration has changed on disk
	migrationsMap := map[uint64]*migration{}
	migratedMap := map[uint64]*migration{}
	for _, m := range migrations {
		migrationsMap[m.Version] = m
	}
	for _, m := range migrated {
		migratedMap[m.Version] = m
	}
	changedMigrations := []*migration{}
	for _, fm := range migrations {
		dm, found := migratedMap[fm.Version]
		if !found {
			continue
		}
		if dm.Up != fm.Up || dm.Down != fm.Down {
			// found a migration that has been changed on disk
			changedMigrations = append(changedMigrations, fm)
		}
	}
	if len(changedMigrations) > 0 {
		//
	}
	migratedOnlyInDb := migrationSliceDifference(migrated, migrations)
	if len(migratedOnlyInDb) > 0 {
		// we have migrations that don't exist in file system

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
	ctx.dbg("DumpDBSchemaToFileWithName")
	schema, err := ctx.DumpDBSchema()
	if err != nil {
		return err
	}
	migrations, err := ctx.dbGetMigrated()
	if err != nil {
		return err
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

// LoadDBSchema loads specified schema and inserts migrations from matching
// migrations file if found next to the schema sql.
func (ctx *PGMigrate) LoadDBSchema(schemaName string, cb ConfirmCB) error {
	ctx.dbg("LoadDBSchema")
	schemaContents, err := ctx.fileGetContents(schemaName)
	if err != nil {
		ctx.dbg("LoadDBSchema", err)
		return err
	}
	// find matching migrations file
	re := regexp.MustCompilePOSIX("^(schema)")
	migrateName := re.ReplaceAllString(schemaName, "migrations")
	ctx.dbg("LoadDBSchema", migrateName)
	migrations := []*migration{}
	migrateContents, err := ctx.fileGetContents(migrateName)
	if err == nil {
		if cb != nil && cb(fmt.Sprintf("found a migrations file '%s'. should these migrations be inserted?", migrateName)) {
			if err := json.Unmarshal([]byte(migrateContents), &migrations); err != nil {
				ctx.dbg("LoadDBSchema", err)
				return err
			}
			sort.Sort(byVersion(migrations))
		}
	}
	ctx.logger.Inf(fmt.Sprintf("loading %s", schemaName))
	if err := ctx.dbExecString(schemaContents, nil); err != nil {
		ctx.dbg("LoadDBSchema", err)
		return err
	}
	if err := ctx.dbInsertMigrationBatch(migrations); err != nil {
		ctx.dbg("LoadDBSchema", err)
		return err
	}
	ctx.dbg("LoadDBSchema", "done")
	return nil
}

func getFileNameOrDefault(prefix, suffix string, fname *string, t *int64) string {
	if fname != nil {
		return fmt.Sprintf("%s_%s.%s", prefix, *fname, suffix)
	}
	if t != nil {
		return fmt.Sprintf("%s_%d.%s", prefix, *t, suffix)
	}
	now := time.Now().Unix()
	return fmt.Sprintf("%s_%d.%s", prefix, now, suffix)
}

// DumpDBSchemaToFile dumps database schema and performed database migrations to files named `schema_<timestamp-unix>.sql` and `migrations_<timestamp-unix>.sql`.
func (ctx *PGMigrate) DumpDBSchemaToFile(fname *string) error {
	now := time.Now().Unix()
	schemaName := getFileNameOrDefault("schema", "sql", fname, &now)
	migrationsName := getFileNameOrDefault("migrations", "sql", fname, &now)
	return ctx.DumpDBSchemaToFileWithName(schemaName, migrationsName)
}

// DumpDBFull dumps database schema and content to a file named `dump_<timestamp-unix>.sql`
func (ctx *PGMigrate) DumpDBFull(fname *string) error {
	ctx.dbg("dumpDBData")
	cmd := exec.Command("pg_dump", ctx.config.DBUrl, "-O", "--column-inserts")
	if err := ctx.fileEnsureDirExist(ctx.config.BaseDirectory); err != nil {
		return err
	}
	fp := filepath.Join(ctx.config.BaseDirectory, getFileNameOrDefault("dump", "sql", fname, nil))
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
	ctx.logger.Ok(fmt.Sprintf("database dump written to \"%s\"", fp))
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
	for i := range kvs {
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
	ctx.logger.Ok(fmt.Sprintf("database restored successfully from '%s'", dumpName))
	return nil
}
