package pgmigrate

import (
	"fmt"
	"io/ioutil"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type migration struct {
	Version uint64 `db:"version"`
	Name    string `db:"name"`
	Up      string `db:"up"`
	Down    string `db:"down"`
}

var reMigrationName = regexp.MustCompilePOSIX("^[a-z0-9][a-z0-9_]+$")

type byVersion []*migration

func (a byVersion) Len() int           { return len(a) }
func (a byVersion) Less(i, j int) bool { return a[i].Version < a[j].Version }
func (a byVersion) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func (ctx *PGMigrate) migrationGetVersion(fileName string) (uint64, error) {
	ctx.dbg("migrationGetVersion", fileName)
	fs := strings.Split(fileName, "_")
	return strconv.ParseUint(fs[0], 10, 64)
}

func (ctx *PGMigrate) migrationGetAll() ([]*migration, error) {
	ctx.dbg("migrationGetAll")
	files, err := ioutil.ReadDir(ctx.config.BaseDirectory)
	if err != nil {
		ctx.dbg("migrationGetAll", err)
		return nil, err
	}
	reg := regexp.MustCompilePOSIX("^.*(up|down).sql$")
	migMap := map[uint64]*migration{}
	for _, fo := range files {
		if fo.IsDir() {
			continue
		}
		sm := reg.FindStringSubmatch(fo.Name())
		if len(sm) != 2 {
			continue
		}
		ctx.dbg("migrationGetAll", sm)
		direction := sm[1]
		version, err := ctx.migrationGetVersion(fo.Name())
		if err != nil {
			ctx.dbg("migrationGetAll", err)
			return nil, err
		}
		contents, err := ctx.fileGetContents(fo.Name())
		migNameParts := strings.Split(fo.Name(), ".")
		migrationName := strings.Join(migNameParts[0:len(migNameParts)-2], ".")
		if m, found := migMap[version]; found {
			if direction == "up" {
				m.Up = contents
			} else {
				m.Down = contents
			}
		} else {
			m := &migration{
				Version: version,
				Name:    migrationName,
			}
			if direction == "up" {
				m.Up = contents
			} else {
				m.Down = contents
			}
			migMap[version] = m
		}
	}
	migrations := []*migration{}
	for _, m := range migMap {
		migrations = append(migrations, m)
	}
	ctx.dbg("migrationGetAll", "sorting migrations")
	sort.Sort(byVersion(migrations))
	return migrations, nil
}

func (ctx *PGMigrate) migrationSuperSet(vs1, vs2 []*migration) (r1 []*migration) {
	ctx.dbg("migrationSuperSet")
	for _, i := range vs1 {
		found := false
		for _, j := range vs2 {
			if i.Version == j.Version {
				found = true
				continue
			}
		}
		if !found {
			r1 = append(r1, i)
		}
	}
	return
}

func (ctx *PGMigrate) migrationCreate(name string) error {
	ctx.dbg("migrationCreate", name)
	if ok := reMigrationName.MatchString(name); !ok {
		return fmt.Errorf("invalid migration name, must match the regexp: ^[a-z0-9_]+$")
	}
	epoch := strconv.FormatInt(time.Now().Unix(), 10)
	down := epoch + "_" + name + ".down.sql"
	up := epoch + "_" + name + ".up.sql"
	ctx.logger.Print(fmt.Sprintf("creating %s", down))
	if err := ctx.fileWriteContents(down, []byte("")); err != nil {
		return err
	}
	ctx.logger.Print(fmt.Sprintf("creating %s", up))
	return ctx.fileWriteContents(up, []byte(""))
}