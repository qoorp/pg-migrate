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

	"github.com/andreyvit/diff"
	"github.com/lib/pq"
)

// CreateMigration creates a new migration file with specified name
func (ctx *PQMigrate) CreateMigration(name string) error {
	ctx.dbg("CreateMigration", name)
	return ctx.migrationCreate(name)
}

// Finish commits lingering database transaction (if all in one transaction specified)
// and closes database handle.
func (ctx *PQMigrate) Finish() error {
	ctx.dbg("Finish")
	return ctx.finish()
}

// CreateDB ensures that the database specified in the postgres url
// exists. If not it creates it. This probably won't work if you don't have
// full access to the postgres server.
func (ctx *PQMigrate) CreateDB(cb ConfirmCB) error {
	ctx.dbg("CreateDB")
	return ctx.dbEnsureDBExists(cb)
}

// DropDB drops the database specified in the postgres url.
// This probably won't work if you don't have full access to
// the postgres server.
func (ctx *PQMigrate) DropDB(cb ConfirmCB) error {
	ctx.dbg("DropDB")
	return ctx.dbDropDB(cb)
}

// MigrateFromFile loads the specified file and does a direct migration without
// modifying the migrations table. Useful for database schema
// and database seeds.
func (ctx *PQMigrate) MigrateFromFile(fileName string) error {
	ctx.dbg("MigrateFromFile", fileName)
	return ctx.fileExec(fileName)
}

// New creates a new PQMigrate instance.
func New(config Config) *PQMigrate {
	return ctxNew(config)
}

// MigrateUp applies `up` migrations from migration dir in order.
// `steps` are number of migrations to perform. If steps == -1
// all `up` migrations will be applied.
func (ctx *PQMigrate) MigrateUp(steps int) error {
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
		ctx.logger.Inf("there was nothing to migrate")
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
		if err := ctx.dbMigrate(m, migrateUp); err != nil {
			ctx.dbg("MigrateUp", err)
			return err
		}
		stepsLeft--
	}
	return nil
}

// MigrateDown applies `down` migrations from migration dir in order.
// `steps` are number of migrations to perform. If steps == -1
// all `down` migrations will be applied.
func (ctx *PQMigrate) MigrateDown(steps int) error {
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
		ctx.logger.Inf("there was nothing to migrate")
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

// Sync tries to synchronize db and fs state by first
// checking if any migrations have changed on fs, then
// finding migrations that exist in db but not in fs.
// The last step is to apply all migrations that only
// only exist in fs.
func (ctx *PQMigrate) Sync(cb ConfirmCB) error {
	if cb == nil {
		return fmt.Errorf("Sync is only usable interactively")
	}
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
		// we have migrations that have changed on disk
		ctx.logger.Warn(fmt.Sprintf("%d migration(s) changed on disk!", len(changedMigrations)))
		for _, migration := range changedMigrations {
			ctx.logger.Warn(migration.Name)
			dbMigration := migratedMap[migration.Version]
			if lines := diff.LineDiffAsLines(dbMigration.Up, migration.Up); len(lines) > 0 {
				ctx.logger.Warn("================\nUP:\n================")
				for _, line := range lines {
					if line[0] == '-' {
						ctx.logger.Warn(line)
					} else if line[0] == '+' {
						ctx.logger.Inf(line)
					} else {
						ctx.logger.Ok(line)
					}
				}
			}
			if lines := diff.LineDiffAsLines(dbMigration.Down, migration.Down); len(lines) > 0 {
				ctx.logger.Warn("================\nDOWN:\n================")
				for _, line := range lines {
					if line[0] == '-' {
						ctx.logger.Warn(line)
					} else if line[0] == '+' {
						ctx.logger.Inf(line)
					} else {
						ctx.logger.Ok(line)
					}
				}
			}
			if !cb("Shall i migrate down and then up?") {
				ctx.logger.Warn("skipping migration...")
				continue
			}
			if err := ctx.dbMigrate(dbMigration, migrateDown); err != nil {
				ctx.logger.Error(err)
				return err
			}
			if err := ctx.dbMigrate(migration, migrateUp); err != nil {
				ctx.logger.Error(err)
				return err
			}
		}
	}
	migratedOnlyInDb := migrationSliceDifference(migrated, migrations)
	if len(migratedOnlyInDb) > 0 {
		// we have migrations that don't exist in file system
		ctx.logger.Warn(fmt.Sprintf("%d migrations don't exist on disk", len(migratedOnlyInDb)))
		sort.Sort(byVersionReversed(migratedOnlyInDb))
		for _, migration := range migratedOnlyInDb {
			ctx.logger.Warn("================\nUP:\n================")
			ctx.logger.Ok(migration.Up)
			ctx.logger.Warn("================\nDOWN:\n================")
			ctx.logger.Ok(migration.Down)
			if !cb("Shall i migrate down?") {
				ctx.logger.Warn("skipping migration...")
				continue
			}
			if err := ctx.dbMigrate(migration, migrateDown); err != nil {
				return err
			}
		}
	}
	newMigrations := migrationSliceDifference(migrations, migrated)
	if len(newMigrations) > 0 {
		ctx.logger.Inf(fmt.Sprintf("applying %d new migration(s)", len(newMigrations)))
		sort.Sort(byVersion(newMigrations))
		for _, migration := range newMigrations {
			if err := ctx.dbMigrate(migration, migrateUp); err != nil {
				return err
			}
		}
	} else {
		ctx.logger.Inf("nothing to migrate")
	}
	return nil
}

// DumpDBSchema dumps the database schema without owner information.
func (ctx *PQMigrate) DumpDBSchema() ([]byte, error) {
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
func (ctx *PQMigrate) DumpDBSchemaToFileWithName(schemaName, migrationsName string) error {
	ctx.dbg("DumpDBSchemaToFileWithName")
	schema, err := ctx.DumpDBSchema()
	if err != nil {
		return err
	}
	migrations, err := ctx.dbGetMigrated()
	if err != nil && !strings.Contains(err.Error(), "pq: relation") {
		return err
	}
	if len(migrations) > 0 {
		jBytes, err := json.Marshal(migrations)
		if err != nil {
			ctx.dbg("DumpDBSchemaToFileWithName", err)
			return err
		}
		if err := ctx.fileWriteContents(migrationsName, jBytes); err != nil {
			ctx.dbg("DumpDBSchemaToFileWithName", err)
			return err
		}
	}
	if err := ctx.fileWriteContents(schemaName, schema); err != nil {
		ctx.dbg("DumpDBSchemaToFileWithName", err)
		return err
	}
	return nil
}

// LoadDBSchema loads specified schema and inserts migrations from matching
// migrations file if found next to the schema sql.
func (ctx *PQMigrate) LoadDBSchema(schemaName string, cb ConfirmCB) error {
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
	insertMigrations := false
	if err == nil {
		if cb != nil && cb(fmt.Sprintf("found a migrations file '%s'. should these migrations be inserted?", migrateName)) {
			if err := json.Unmarshal([]byte(migrateContents), &migrations); err != nil {
				ctx.dbg("LoadDBSchema", err)
				return err
			}
			sort.Sort(byVersion(migrations))
			insertMigrations = true
		}
	}
	ctx.logger.Inf(fmt.Sprintf("loading %s", schemaName))
	if err := ctx.dbExecString(schemaContents, nil); err != nil {
		ctx.dbg("LoadDBSchema", err)
		return err
	}
	if insertMigrations {
		ctx.logger.Inf(fmt.Sprintf("inserting %d migration(s)", len(migrations)))
		if err := ctx.dbInsertMigrationBatch(migrations); err != nil {
			ctx.dbg("LoadDBSchema", err)
			return err
		}
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
func (ctx *PQMigrate) DumpDBSchemaToFile(fname *string) error {
	now := time.Now().Unix()
	schemaName := getFileNameOrDefault("schema", "sql", fname, &now)
	migrationsName := getFileNameOrDefault("migrations", "sql", fname, &now)
	return ctx.DumpDBSchemaToFileWithName(schemaName, migrationsName)
}

// DumpDBFull dumps database schema and content to a file named `dump_<timestamp-unix>.sql`
func (ctx *PQMigrate) DumpDBFull(fname *string) error {
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

func (ctx *PQMigrate) LoadFullDump(dumpName string) error {
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
